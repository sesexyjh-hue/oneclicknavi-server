#!/usr/bin/env python3
"""원클릭내비 서버 — WebSocket 연결 브로커 + Kakao Geocoding Proxy"""

import asyncio
import json
import logging
import os
import uuid
from datetime import datetime

import aiohttp
from aiohttp import web

# ── 설정 ──
HOST = "0.0.0.0"
PORT = int(os.environ.get("PORT", "8090"))
KAKAO_REST_KEY = os.environ.get("KAKAO_REST_KEY", "ea87c8a2c04fdf83b5bbdd1fc2f31efa")
KAKAO_GEOCODE_URL = "https://dapi.kakao.com/v2/local/search/address.json"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("navi-server")

# ── 상태 ──
clients: dict[str, dict] = {}        # device_id → {"ws": WebSocketResponse, "role": "sender"|"receiver", "pair": set, "email": str}
devices: dict[str, str] = {}         # device_id → email
pairs: dict[str, set] = {}           # sender_email → set(connected_ws)
total_locations = 0
start_time = datetime.now()


# ── Kakao Geocoding ──
async def geocode_address(session: aiohttp.ClientSession, address: str) -> dict | None:
    """주소 → 위도/경도 변환. 실패 시 None."""
    headers = {"Authorization": f"KakaoAK {KAKAO_REST_KEY}"}
    params = {"query": address, "analyze_type": "similar"}

    try:
        async with session.get(KAKAO_GEOCODE_URL, headers=headers, params=params, timeout=aiohttp.ClientTimeout(total=5)) as resp:
            if resp.status != 200:
                log.warning(f"Kakao API error: {resp.status} for address={address}")
                return None
            data = await resp.json()
            docs = data.get("documents", [])
            if not docs:
                log.warning(f"No Kakao results for: {address}")
                return None

            doc = docs[0]
            lat = float(doc.get("y", 0))
            lng = float(doc.get("x", 0))
            road_addr = doc.get("road_address", {}).get("address_name", "") if doc.get("road_address") else ""
            jibun_addr = doc.get("address", {}).get("address_name", "")

            return {
                "latitude": lat,
                "longitude": lng,
                "address": road_addr or jibun_addr or address,
                "source": "kakao",
            }
    except asyncio.TimeoutError:
        log.warning(f"Kakao geocode timeout: {address}")
        return None
    except Exception as e:
        log.error(f"Kakao geocode error: {e}")
        return None


# ── WebSocket ──
async def handle_websocket(request: web.Request) -> web.WebSocketResponse:
    ws = web.WebSocketResponse(max_msg_size=512 * 1024)
    await ws.prepare(request)

    device_id = str(uuid.uuid4())[:8]
    email = "unknown"
    role = "sender"
    clients[device_id] = {"ws": ws, "role": role, "email": email}
    log.info(f"Client connected: {device_id} (total: {len(clients)})")

    try:
        async for msg in ws:
            if msg.type == aiohttp.WSMsgType.TEXT:
                data = json.loads(msg.data)
                await handle_message(device_id, data)

            elif msg.type == aiohttp.WSMsgType.ERROR:
                log.error(f"WS error [{device_id}]: {ws.exception()}")
                break
    except Exception as e:
        log.error(f"WS exception [{device_id}]: {e}")
    finally:
        if device_id in clients:
            del clients[device_id]
        # Clean up pairs
        for sender_email in list(pairs.keys()):
            pairs[sender_email].discard(device_id)
            if not pairs[sender_email]:
                del pairs[sender_email]
        log.info(f"Client disconnected: {device_id} (total: {len(clients)})")

    return ws


async def handle_message(sender_id: str, data: dict):
    global total_locations
    msg_type = data.get("type", "unknown")
    email = data.get("email") or data.get("googleAccount", "unknown")
    device_id = data.get("device_id") or data.get("deviceId", sender_id)

    # Update client info
    if sender_id in clients:
        clients[sender_id]["email"] = email

    log.info(f"Msg [{sender_id}]: type={msg_type}, email={email}, role={data.get('role', 'none')}")
    log.info(f"RAW MSG: {json.dumps(data, ensure_ascii=False)[:300]}")

    if msg_type == "register":
        # Register device as sender/receiver
        role = data.get("role", "sender")
        if sender_id in clients:
            clients[sender_id]["role"] = role
        devices[device_id] = email
        log.info(f"Registered {device_id} as {role} (email={email})")

        # Send registration confirmation
        if sender_id in clients:
            ws = clients[sender_id]["ws"]
            await ws.send_json({"type": "registered", "device_id": device_id, "role": role})

        # Auto-pair: receiver adds itself to pairs[email]
        # When sender sends location_data, server looks up pairs[sender_email] for receivers
        if role == "receiver":
            if email not in pairs:
                pairs[email] = set()
            pairs[email].add(sender_id)
            log.info(f"Auto-paired receiver {sender_id} -> pairs[{email}] (email={email})")

        await broadcast_status()

    elif msg_type == "register_sender":
        if sender_id in clients:
            clients[sender_id]["role"] = "sender"
        # Auto-pair: find receivers with same email and add to pair set
        client_email = clients.get(sender_id, {}).get("email", email)
        if client_email not in pairs:
            pairs[client_email] = set()
        for cid, info in list(clients.items()):
            if cid != sender_id and info.get("email") == client_email and info.get("role") == "receiver":
                pairs[client_email].add(cid)
        log.info(f"Auto-paired sender {sender_id} (email={client_email}), receivers: {len(pairs.get(client_email, set()))}")
        await broadcast_status()

    elif msg_type == "register_receiver":
        if sender_id in clients:
            clients[sender_id]["role"] = "receiver"
        # Auto-pair: add this receiver to all senders with same email
        client_email = clients.get(sender_id, {}).get("email", email)
        was_auto_paired = False
        for cid, info in list(clients.items()):
            if cid != sender_id and info.get("email") == client_email and info.get("role") == "sender":
                if client_email not in pairs:
                    pairs[client_email] = set()
                pairs[client_email].add(sender_id)
                was_auto_paired = True
        log.info(f"Auto-paired receiver {sender_id} (email={client_email}, auto_paired={was_auto_paired})")
        await broadcast_status()

    elif msg_type == "location_data":
        total_locations += 1
        # Kakao geocoding: address → lat/lng
        address = data.get("address") or data.get("locationInfo") or data.get("location_info", "")
        location_info = data.get("locationInfo", address)

        if isinstance(location_info, dict):
            location_text = location_info.get("address", str(location_info))
        else:
            location_text = str(location_info)

        # Try Kakao geocoding
        async with aiohttp.ClientSession() as session:
            geo = await geocode_address(session, location_text)

        if geo:
            data["latitude"] = geo["latitude"]
            data["longitude"] = geo["longitude"]
            data["address"] = geo["address"]
            log.info(f"Geocoded: {location_text[:30]} → ({geo['latitude']:.4f}, {geo['longitude']:.4f})")
        else:
            log.warning(f"Geocoding failed for: {location_text[:50]}")
            # Nominatim fallback
            try:
                async with aiohttp.ClientSession() as session:
                    nom_url = f"https://nominatim.openstreetmap.org/search?q={location_text}&format=json&limit=1"
                    async with session.get(nom_url, headers={"User-Agent": "OneClickNavi/1.0"}, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                        if resp.status == 200:
                            nom_data = await resp.json()
                            if nom_data:
                                data["latitude"] = float(nom_data[0]["lat"])
                                data["longitude"] = float(nom_data[0]["lon"])
                                log.info(f"Nom fallback: ({data['latitude']:.4f}, {data['longitude']:.4f})")
            except Exception as e:
                log.error(f"Nom fallback error: {e}")

        # Broadcast to sender's paired receivers (with app's expected format)
        sender_email = email
        if sender_email in pairs:
            # Extract nested data from sender's message
            nested_data = data.get("data", data)
            # Remove protocol fields that shouldn't be in relay
            if isinstance(nested_data, dict):
                nested_data.pop("type", None)
            relay_msg = {
                "type": "location_data",
                "googleAccount": sender_email,
                "data": nested_data
            }
            for target_id in list(pairs[sender_email]):
                if target_id in clients:
                    target_ws = clients[target_id]["ws"]
                    try:
                        await target_ws.send_json(relay_msg)
                    except Exception as e:
                        log.error(f"Broadcast error to {target_id}: {e}")
                        pairs[sender_email].discard(target_id)

    elif msg_type == "pair":
        # Pair sender with all receivers sharing the same email
        sender_email = data.get("sender_email", email)
        if sender_email not in pairs:
            pairs[sender_email] = set()
        # Find all receivers with the same email and add to pair set
        paired_count = 0
        for cid, info in list(clients.items()):
            if info.get("email") == sender_email and info.get("role") == "receiver":
                if cid not in pairs[sender_email]:
                    pairs[sender_email].add(cid)
                    paired_count += 1
        log.info(f"Paired sender {sender_id} -> {paired_count} receivers (email={sender_email}), total paired: {len(pairs[sender_email])}")

        if sender_id in clients:
            ws = clients[sender_id]["ws"]
            await ws.send_json({"type": "paired", "sender_email": sender_email, "receivers": paired_count})

        await broadcast_status()

    elif msg_type == "get_clients":
        # Return client list
        if sender_id in clients:
            ws = clients[sender_id]["ws"]
            client_list = []
            for cid, info in clients.items():
                if info.get("ws") and not info["ws"].closed:
                    client_list.append({
                        "device_id": cid,
                        "email": info["email"],
                        "role": info.get("role", "unknown"),
                    })
            await ws.send_json({"type": "client_list", "clients": client_list})

    elif msg_type == "sender_count_request":
        # Send sender count
        if sender_id in clients:
            ws = clients[sender_id]["ws"]
            count = len(pairs.get(email, set()))
            await ws.send_json({"type": "sender_count_response", "count": count})

    elif msg_type == "ping":
        if sender_id in clients:
            ws = clients[sender_id]["ws"]
            await ws.send_json({"type": "pong"})


async def broadcast_status():
    """Broadcast receiver count to all clients (app expects 'receiver_count' type)."""
    for cid, info in list(clients.items()):
        if info.get("ws") and not info["ws"].closed:
            ws = info["ws"]
            try:
                email = info["email"]
                count = len(pairs.get(email, set()))
                await ws.send_json({"type": "receiver_count", "count": count})
            except Exception:
                pass


# ── HTTP Routes ──
async def handle_root(request: web.Request) -> web.Response:
    html = f"""<!DOCTYPE html>
<html lang="ko">
<head><meta charset="utf-8"><title>원클릭내비 서버</title>
<style>
body {{ font-family: sans-serif; max-width: 800px; margin: 40px auto; padding: 0 20px; background: #1a1a1a; color: #eee; }}
h1 {{ color: #e53935; }}
.status {{ border: 1px solid #333; border-radius: 8px; padding: 16px; margin: 12px 0; }}
.status dt {{ color: #e53935; font-weight: bold; }}
.status dd {{ margin-left: 16px; }}
.clients {{ margin-top: 20px; }}
.client {{ background: #2d2d2d; border-radius: 8px; padding: 8px 12px; margin: 4px 0; }}
</style></head>
<body>
<h1>🏎️ 원클릭내비 서버</h1>
<div class="status">
<dl>
<dt>상태</dt><dd>✅ 실행 중</dd>
<dt>접속 클라이언트</dt><dd>{len(clients)}</dd>
<dt>총 위치 전송</dt><dd>{total_locations}</dd>
<dt>가동 시간</dt><dd>{(datetime.now() - start_time).seconds}초</dd>
</dl>
</div>
<div class="clients">
<h2>접속 중인 기기</h2>
{''.join(f'<div class="client">📱 {info["email"][:20]} ({info.get("role","?")})</div>' for cid, info in sorted(clients.items())) if clients else '<p>접속 없음</p>'}
</div>
</body></html>"""
    return web.Response(text=html, content_type="text/html")


async def handle_health(request: web.Request) -> web.Response:
    info = {
        "status": "running",
        "clients": len(clients),
        "devices": list(devices.keys()),
        "total_locations": total_locations,
        "uptime_seconds": (datetime.now() - start_time).seconds,
    }
    # Client details
    client_info = {}
    for cid, cdata in list(clients.items()):
        client_info[cid] = {"email": cdata.get("email"), "role": cdata.get("role")}
    info["client_details"] = client_info
    info["pairs"] = {k: list(v) for k, v in pairs.items()}
    return web.json_response(info)


# ── REST API ──
def api_ok(data=None):
    return web.json_response({"success": True, "data": data, "message": "ok"})

def api_err(msg="error"):
    return web.json_response({"success": False, "data": None, "message": msg})

async def handle_google_auth(request: web.Request) -> web.Response:
    """POST /api/v1/auth/google — Google 로그인 처리"""
    try:
        body = await request.json()
    except Exception:
        body = {}
    email = body.get("email", body.get("idToken", "unknown"))
    log.info(f"Google auth: {email}")
    # 발신/수신 여부는 WebSocket에서 결정되므로 여기선 아무 유저나 허용
    return api_ok({
        "access_token": f"mock_access_{uuid.uuid4().hex[:16]}",
        "refresh_token": f"mock_refresh_{uuid.uuid4().hex[:16]}",
        "user": {
            "googleId": body.get("uid", "mock_google_id"),
            "email": email,
            "displayName": body.get("displayName", email.split("@")[0] if "@" in email else email),
            "photoUrl": body.get("photoUrl", ""),
            "subscriptionStatus": "active",
            "role": "sender",
        }
    })

async def handle_auth_refresh(request: web.Request) -> web.Response:
    """POST /api/v1/auth/refresh — 토큰 갱신"""
    return api_ok({
        "access_token": f"mock_access_{uuid.uuid4().hex[:16]}",
        "refresh_token": f"mock_refresh_{uuid.uuid4().hex[:16]}",
    })

async def handle_get_profile(request: web.Request) -> web.Response:
    """GET /api/v1/users/me — 내 프로필"""
    # 헤더에서 이메일 추출 시도
    auth = request.headers.get("Authorization", "")
    email = "user@email.com"
    if auth and len(auth) > 10:
        # Bearer 토큰으로 유저 찾기 생략 — mock 응답
        pass
    return api_ok({
        "googleId": "mock_google_id",
        "email": email,
        "displayName": "사용자",
        "photoUrl": "",
        "subscriptionStatus": "active",
        "role": "sender",
    })

async def handle_subscription_status(request: web.Request) -> web.Response:
    """GET /api/v1/payments/subscription/status — 구독 상태"""
    return api_ok({
        "status": "active",
        "plan": "premium",
        "expiresAt": "2099-12-31T23:59:59Z",
        "remainingDays": 9999,
    })

async def handle_price(request: web.Request) -> web.Response:
    """GET /api/v1/payments/price — 가격"""
    return api_ok({
        "amount": 0,
        "currency": "KRW",
        "label": "무료",
    })

async def handle_maintenance(request: web.Request) -> web.Response:
    """GET /api/v1/system/maintenance — 점검 상태"""
    return api_ok({"maintenance": False})

async def handle_withdraw(request: web.Request) -> web.Response:
    """DELETE /api/v1/auth/withdraw — 회원 탈퇴"""
    return api_ok({"withdrawn": True})

async def handle_payment_action(request: web.Request) -> web.Response:
    """POST api/v1/payments/* — 더미 결제 처리"""
    return api_ok({"success": True, "orderId": f"order_{uuid.uuid4().hex[:12]}"})

async def handle_cancel_subscription(request: web.Request) -> web.Response:
    """POST api/v1/payments/subscription/cancel — 구독 취소"""
    return api_ok({"cancelled": True})

async def handle_api_404(request: web.Request) -> web.Response:
    """앱에서 찾는 API가 없을 때 mock 응답"""
    path = request.path
    log.warning(f"Unknown API: {request.method} {path}")
    return api_err(f"endpoint not found: {path}")


# ── App ──
app = web.Application()

# Public
app.router.add_get("/", handle_root)
app.router.add_get("/health", handle_health)
app.router.add_get("/ws", handle_websocket)

# Auth
app.router.add_post("/api/v1/auth/google", handle_google_auth)
app.router.add_post("/api/v1/auth/refresh", handle_auth_refresh)
app.router.add_delete("/api/v1/auth/withdraw", handle_withdraw)

# User
app.router.add_get("/api/v1/users/me", handle_get_profile)

# Payment
app.router.add_get("/api/v1/payments/price", handle_price)
app.router.add_get("/api/v1/payments/subscription/status", handle_subscription_status)
app.router.add_post("/api/v1/payments/confirm", handle_payment_action)
app.router.add_post("/api/v1/payments/orders", handle_payment_action)
app.router.add_post("/api/v1/payments/billing/issue", handle_payment_action)
app.router.add_post("/api/v1/payments/subscription/cancel", handle_cancel_subscription)

# System
app.router.add_get("/api/v1/system/maintenance", handle_maintenance)

# Catch-all for /api/v1/* routes (must be last)
app.router.add_route("*", "/api/v1/{tail:.*}", handle_api_404)

if __name__ == "__main__":
    log.info(f"Starting OneClickNavi server on {HOST}:{PORT}")
    web.run_app(app, host=HOST, port=PORT)
