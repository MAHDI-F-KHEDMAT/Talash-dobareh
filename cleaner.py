import os
import re
import sys
import time
import math
import json
import base64
import socket
import threading
import subprocess
import asyncio
from queue import Queue
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests

# لیست آدرس‌های سورس کانفیگ‌ها
URLS = [
    "https://raw.githubusercontent.com/itsyebekhe/PSG/main/subscriptions/xray/base64/mix",
    "https://raw.githubusercontent.com/shaoyouvip/free/refs/heads/main/base64.txt",
    "https://raw.githubusercontent.com/telegeam/freenode/refs/heads/master/v2ray.txt",
    "https://raw.githubusercontent.com/DukeMehdi/FreeList-V2ray-Configs/refs/heads/main/Configs/VLESS-V2Ray-Configs-By-DukeMehdi.txt",
    "https://raw.githubusercontent.com/Flikify/Free-Node/refs/heads/main/v2ray.txt",
    "https://raw.githubusercontent.com/RaitonRed/ConfigsHub/refs/heads/main/Splitted-By-Protocol/vless.txt",
    "https://raw.githubusercontent.com/shuaidaoya/FreeNodes/refs/heads/main/nodes/base64.txt",
    "https://raw.githubusercontent.com/penhandev/AutoAiVPN/refs/heads/main/allConfigs.txt",
    "https://raw.githubusercontent.com/Firmfox/Proxify/refs/heads/main/v2ray_configs/seperated_by_protocol/vless.txt",
    "https://raw.githubusercontent.com/crackbest/V2ray-Config/refs/heads/main/config.txt",
    "https://raw.githubusercontent.com/kismetpro/NodeSuber/refs/heads/main/Splitted-By-Protocol/vless.txt",
    "https://raw.githubusercontent.com/jagger235711/V2rayCollector/refs/heads/main/results/vless.txt",
    "https://raw.githubusercontent.com/mohamadfg-dev/telegram-v2ray-configs-collector/refs/heads/main/category/vless.txt",
    "https://raw.githubusercontent.com/SoroushImanian/BlackKnight/refs/heads/main/sub/vless",
    "https://raw.githubusercontent.com/Matin-RK0/ConfigCollector/refs/heads/main/subscription.txt",
    "https://raw.githubusercontent.com/Argh73/VpnConfigCollector/refs/heads/main/All_Configs_Sub.txt",
    "https://raw.githubusercontent.com/3yed-61/configs-collector/refs/heads/main/classified_output/vless.txt",
    "https://raw.githubusercontent.com/Leon406/SubCrawler/refs/heads/main/sub/share/vless",
    "https://raw.githubusercontent.com/ircfspace/XraySubRefiner/refs/heads/main/export/soliSpirit/normal",
    "https://raw.githubusercontent.com/ircfspace/XraySubRefiner/refs/heads/main/export/psgV6/normal",
    "https://raw.githubusercontent.com/ircfspace/XraySubRefiner/refs/heads/main/export/psgMix/normal",
    "https://raw.githubusercontent.com/MhdiTaheri/V2rayCollector_Py/refs/heads/main/sub/Mix/mix.txt",
    "https://raw.githubusercontent.com/T3stAcc/V2Ray/refs/heads/main/Splitted-By-Protocol/vless.txt",
    "https://raw.githubusercontent.com/F0rc3Run/F0rc3Run/refs/heads/main/splitted-by-protocol/vless.txt",
    "https://raw.githubusercontent.com/V2RayRoot/V2RayConfig/refs/heads/main/Config/vless.txt",
    "https://raw.githubusercontent.com/LalatinaHub/Mineral/refs/heads/master/result/nodes",
    "https://raw.githubusercontent.com/barry-far/V2ray-Config/refs/heads/main/All_Configs_Sub.txt",
    "https://raw.githubusercontent.com/hamedcode/port-based-v2ray-configs/refs/heads/main/sub/vless.txt",
    "https://raw.githubusercontent.com/iboxz/free-v2ray-collector/refs/heads/main/main/vless",
    "https://raw.githubusercontent.com/Epodonios/v2ray-configs/refs/heads/main/Splitted-By-Protocol/vless.txt",
    "https://raw.githubusercontent.com/ebrasha/free-v2ray-public-list/refs/heads/main/vless_configs.txt",
    "https://raw.githubusercontent.com/Pasimand/v2ray-config-agg/refs/heads/main/config.txt",
    "https://raw.githubusercontent.com/arshiacomplus/v2rayExtractor/refs/heads/main/vless.html",
    "https://raw.githubusercontent.com/xyfqzy/free-nodes/refs/heads/main/nodes/vless.txt",
    "https://raw.githubusercontent.com/AvenCores/goida-vpn-configs/refs/heads/main/githubmirror/14.txt",
    "https://raw.githubusercontent.com/Awmiroosen/awmirx-v2ray/refs/heads/main/blob/main/v2-sub.txt",
    "https://raw.githubusercontent.com/SoliSpirit/v2ray-configs/refs/heads/main/Protocols/vless.txt",
    "https://raw.githubusercontent.com/gfpcom/free-proxy-list/refs/heads/main/list/vless.txt"
]

# وب‌سایت‌های هدف تست اتصال واقعی
TARGETS = [
    "https://www.instagram.com",
    "https://www.x.com",
    "https://www.youtube.com",
    "https://www.amazon.com",
    "https://www.openai.com"
]

start_time = time.time()
current_stage = "شروع پروژه"
progress_info = "در حال آماده سازی..."

def keep_alive_logger():
    while True:
        time.sleep(300)
        elapsed = int(time.time() - start_time) // 60
        print(f"[LOG - {elapsed}m elapsed] Stage: {current_stage} | Info: {progress_info}", flush=True)

logger_thread = threading.Thread(target=keep_alive_logger, daemon=True)
logger_thread.start()

def log(message):
    print(f"[{time.strftime('%H:%M:%S')}] {message}", flush=True)

def decode_base64_safe(s):
    s = s.strip()
    missing_padding = len(s) % 4
    if missing_padding:
        s += '=' * (4 - missing_padding)
    try:
        return base64.b64decode(s).decode('utf-8', errors='ignore')
    except:
        return ""

def extract_vless_links(text):
    return re.findall(r'vless://[^\s]+', text)

def fetch_url(url):
    try:
        resp = requests.get(url, timeout=15)
        if resp.status_code == 200:
            content = resp.text
            links = extract_vless_links(content)
            if not links:
                decoded = decode_base64_safe(content)
                links = extract_vless_links(decoded)
            if not links:
                for line in content.splitlines():
                    decoded_line = decode_base64_safe(line)
                    links.extend(extract_vless_links(decoded_line))
            return links
    except:
        pass
    return []

# --- بخش بهینه‌سازی شده تست TCP به صورت کاملاً Async ---

async def async_tcp_ping(host, port, timeout=1.5):
    """تست پینگ به صورت غیرمسدودکننده (Async)"""
    try:
        t_start = time.perf_counter()
        coro = asyncio.open_connection(host, port)
        reader, writer = await asyncio.wait_for(coro, timeout=timeout)
        t_end = time.perf_counter()
        writer.close()
        await writer.wait_closed()
        return (t_end - t_start) * 1000
    except:
        return None

async def async_test_ping_and_stddev(node, semaphore):
    """انجام پینگ متوالی با سیستم Fail-Fast برای تصفیه سریع سرورهای مرده"""
    async with semaphore:
        pings = []
        for _ in range(5):
            p = await async_tcp_ping(node["host"], node["port"])
            if p is None:
                return None  # اگر حتی یک بار پینگ قطع شد، فوراً سرور را رد کن (افزایش چشمگیر سرعت)
            pings.append(p)
            await asyncio.sleep(0.05)
        
        mean = sum(pings) / len(pings)
        variance = sum((x - mean) ** 2 for x in pings) / len(pings)
        std_dev = math.sqrt(variance)
        
        if std_dev > 200:
            return None
        return node

async def run_async_pings(parsed_nodes):
    """مدیریت اجرای موازی ۵۰۰ تست پینگ همزمان در رانر گیت‌هاب"""
    global progress_info
    semaphore = asyncio.Semaphore(500)
    tasks = [async_test_ping_and_stddev(node, semaphore) for node in parsed_nodes]
    
    alive_nodes = []
    total_nodes = len(tasks)
    idx = 0
    
    for coro in asyncio.as_completed(tasks):
        res = await coro
        idx += 1
        if res:
            alive_nodes.append(res)
        if idx % 200 == 0 or idx == total_nodes:
            progress_info = f"تست پینگ ناهمگام: {idx}/{total_nodes} انجام شد. زنده: {len(alive_nodes)}"
            log(progress_info)
            
    return alive_nodes

# --- پایان بخش Async ---

def parse_vless(url_str):
    try:
        clean_url = url_str.split('#')[0]
        if '?' in clean_url:
            base_part, query_part = clean_url.split('?', 1)
        else:
            base_part, query_part = clean_url, ""
            
        netloc = base_part.replace("vless://", "")
        if '@' not in netloc:
            return None
        uuid, address_port = netloc.split('@', 1)
        
        if ':' not in address_port:
            return None
        
        if ']' in address_port:
            address = address_port.split(']')[0] + ']'
            port_str = address_port.split(']')[-1].replace(':', '')
        else:
            address, port_str = address_port.split(':', 1)
            
        port = int(port_str)
        
        query = {}
        if query_part:
            for pair in query_part.split('&'):
                if '=' in pair:
                    k, v = pair.split('=', 1)
                    query[k] = v

        network = query.get('type', query.get('network', 'tcp'))
        security = query.get('security', 'none')
        sni = query.get('sni', query.get('peer', ''))
        path = query.get('path', '')
        serviceName = query.get('serviceName', query.get('service_name', ''))
        pbk = query.get('pbk', query.get('publickey', ''))
        sid = query.get('sid', query.get('shortid', ''))
        flow = query.get('flow', '')

        outbound = {
            "protocol": "vless",
            "settings": {
                "vnext": [{
                    "address": address,
                    "port": port,
                    "users": [{"id": uuid, "encryption": "none", "level": 0}]
                }]
            },
            "streamSettings": {"network": network, "security": security}
        }
        
        if flow:
            outbound["settings"]["vnext"][0]["users"][0]["flow"] = flow

        if network == "ws":
            outbound["streamSettings"]["wsSettings"] = {}
            if path: outbound["streamSettings"]["wsSettings"]["path"] = path
            if sni: outbound["streamSettings"]["wsSettings"]["headers"] = {"Host": sni}
        elif network == "grpc":
            outbound["streamSettings"]["grpcSettings"] = {"serviceName": serviceName if serviceName else "Tun"}
        elif network == "tcp" and query.get('headerType') == "http":
            outbound["streamSettings"]["tcpSettings"] = {
                "header": {
                    "type": "http",
                    "request": {"version": "1.1", "method": "GET", "path": [path if path else "/"], "headers": {"Host": [sni] if sni else []}}
                }
            }

        if security in ["tls", "xtls"]:
            outbound["streamSettings"][f"{security}Settings"] = {"serverName": sni if sni else address, "allowInsecure": True}
        elif security == "reality":
            outbound["streamSettings"]["realitySettings"] = {"serverName": sni if sni else address, "publicKey": pbk, "shortId": sid, "fingerprint": "chrome"}

        return {"url": url_str, "host": address, "port": port, "outbound": outbound}
    except:
        return None

def test_xray_node(node, local_port):
    config = {
        "log": {"loglevel": "none"},
        "inbounds": [{
            "port": local_port,
            "listen": "127.0.0.1",
            "protocol": "socks",
            "settings": {"auth": "noauth", "udp": True}
        }],
        "outbounds": [node["outbound"]]
    }
    
    config_path = f"config_{local_port}.json"
    with open(config_path, "w") as f:
        json.dump(config, f)
        
    process = None
    try:
        process = subprocess.Popen(["xray", "run", "-config", config_path], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except:
        try:
            process = subprocess.Popen(["./xray", "run", "-config", config_path], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        except:
            return False

    time.sleep(2.5)
    
    proxies = {
        "http": f"socks5h://127.0.0.1:{local_port}",
        "https": f"socks5h://127.0.0.1:{local_port}"
    }
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
    
    success = True
    for target in TARGETS:
        try:
            # کاهش تایم‌اوت به ۰.۵ ثانیه جهت فیلترینگ فوق‌العاده سخت‌گیرانه برای سرعت‌های فضایی
            resp = requests.get(target, proxies=proxies, headers=headers, timeout=0.5, allow_redirects=False)
            if resp.status_code is None:
                success = False
                break
        except Exception:
            success = False
            break

    if process:
        process.terminate()
        process.wait()
        
    try: os.remove(config_path)
    except: pass
    
    return success

def main():
    global current_stage, progress_info
    
    # مرحله ۱: جمع‌آوری و حذف تکراری‌ها
    current_stage = "مرحله اول: جمع آوری منابع"
    log("شروع جمع‌آوری لینک‌های VLESS...")
    all_raw_links = []
    
    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = {executor.submit(fetch_url, url): url for url in URLS}
        for i, future in enumerate(as_completed(futures), 1):
            res = future.result()
            all_raw_links.extend(res)
            progress_info = f"تعداد سورس‌های بررسی شده: {i}/{len(URLS)}"
            
    unique_links = list(set(all_raw_links))
    log(f"جمع‌آوری به اتمام رسید. کل لینک‌ها: {len(all_raw_links)} | لینک‌های یکتا: {len(unique_links)}")

    parsed_nodes = []
    for link in unique_links:
        parsed = parse_vless(link)
        if parsed:
            parsed_nodes.append(parsed)

    # مرحله ۲ و ۳: فیلترینگ پینگ به روش Async
    current_stage = "مرحله دوم و سوم: فیلتر پینگ و انحراف معیار (Async)"
    log("اجرای پینگ‌های همزمان فوق سریع با معماری غیرمسدودکننده...")
    
    alive_nodes = asyncio.run(run_async_pings(parsed_nodes))
    log(f"پایان تست پینگ سریع. تعداد کانفیگ‌های پایدار اولیه: {len(alive_nodes)}")

    # مرحله ۴: تست اتصال واقعی وبسایت‌ها با Xray Core
    current_stage = "مرحله چهارم: تست اتصال به وبسایت‌های هدف"
    log("شروع تست نهایی اتصال به اینستاگرام، X، یوتیوب، آمازون و OpenAI...")
    
    port_queue = Queue()
    for p in range(15000, 15050):  # تخصیص پورت‌های موازی تا سقف ۵۰ تست همزمان
        port_queue.put(p)
        
    final_configs = []
    
    def worker_xray(node_data):
        port = port_queue.get()
        try:
            is_ok = test_xray_node(node_data, port)
            return node_data["url"] if is_ok else None
        finally:
            port_queue.put(port)

    with ThreadPoolExecutor(max_workers=50) as executor:
        futures = [executor.submit(worker_xray, node) for node in alive_nodes]
        total_xray = len(futures)
        for idx, future in enumerate(as_completed(futures), 1):
            res = future.result()
            if res:
                final_configs.append(res)
            if idx % 50 == 0 or idx == total_xray:
                progress_info = f"تست Xray: {idx}/{total_xray} انجام شد. عبور کرده: {len(final_configs)}"
                log(progress_info)

    # مرحله ۵: ذخیره‌سازی خروجی خروجی نهایی معتبر
    current_stage = "مرحله پنجم: ذخیره خروجی"
    output_filename = "results.txt"
    with open(output_filename, "w", encoding="utf-8") as f:
        for link in final_configs:
            f.write(link + "\n")
            
    log(f"پروژه با موفقیت به پایان رسید! {len(final_configs)} کانفیگ با سرعت ماورایی ذخیره شد.")

if __name__ == "__main__":
    main()
