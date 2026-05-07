import hashlib
import os
import asyncio
from typing import Iterable

import aiohttp
from aiohttp import web

from astrbot.api.event import filter, AstrMessageEvent, MessageEventResult
from astrbot.api.star import Context, Star, register
from astrbot.api import logger
from .data_store import DataStore

@register("acmer_manager", "ACMER", "ACMER 管理插件，支持绑定各种刷题平台账号", "1.0.0")
class ACMerManager(Star):
    def __init__(self, context: Context):
        super().__init__(context)
        # 初始化数据库放在插件目录下
        db_path = os.path.join(os.path.dirname(__file__), "acmer_data.db")
        self.db = DataStore(db_path)
        
        # 提示用户访问 Web 后台
        logger.info("==================================================")
        logger.info(" ACMER Manager 可视化管理后台已就绪")
        logger.info(" 请浏览器访问: http://localhost:0721")
        logger.info("==================================================")
        
        try:
            loop = asyncio.get_event_loop()
            loop.create_task(self._start_web_dashboard())
        except Exception as e:
            logger.error(f"启动可视化后台失败: {e}")

    async def _start_web_dashboard(self):
        app = web.Application()
        app.router.add_get("/", self._web_index)
        
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, 'localhost', 721)
        await site.start()

    async def _web_index(self, request):
        users = self.db.get_all_users()
        
        # 并发获取牛客数据
        async def get_niuke_ac(handle: str) -> int:
            if not handle:
                return 0
            stats = await self._fetch_niuke_stats(handle)
            if stats:
                return stats.get("codingAC", 0) + stats.get("questionAC", 0)
            return 0
            
        niuke_tasks = [get_niuke_ac(u.niuke_handle) for u in users]
        niuke_acs = await asyncio.gather(*niuke_tasks)
        
        # 构建表格行
        table_rows = ""
        for i, user in enumerate(users):
            cf_handle = user.cf_handle or "未绑定"
            cf_rating = user.cf_rating or 0
            # 获取该用户的 CF 过题数
            cf_solved = self.db.count_solved(user.qq_id, "cf") if user.cf_handle else 0
            
            niuke_handle = user.niuke_handle or "未绑定"
            niuke_solved = niuke_acs[i] if user.niuke_handle else 0
            
            table_rows += f"""
                <tr>
                    <td>{user.qq_id}</td>
                    <td>{cf_handle}</td>
                    <td>{cf_rating}</td>
                    <td>{cf_solved}</td>
                    <td>{niuke_handle}</td>
                    <td>{niuke_solved}</td>
                </tr>
            """
            
        html_content = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="utf-8">
            <title>ACMer Manager Dashboard</title>
            <style>
                body {{ font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; padding: 20px; color: #333; }}
                h1 {{ color: #0056b3; }}
                table {{ border-collapse: collapse; width: 100%; max-width: 800px; margin-top: 20px; }}
                th, td {{ border: 1px solid #ddd; padding: 12px; text-align: left; }}
                th {{ background-color: #f8f9fa; font-weight: bold; }}
                tr:nth-child(even) {{ background-color: #f9f9f9; }}
                tr:hover {{ background-color: #f1f1f1; }}
            </style>
        </head>
        <body>
            <h1>ACMer Manager Dashboard</h1>
            <p>可视化管理后台 - 用户数据概览</p>
            <table>
                <tr>
                    <th>用户 ID (QQ)</th>
                    <th>CF 账号</th>
                    <th>CF Rating</th>
                    <th>CF 过题数</th>
                    <th>牛客 ID</th>
                    <th>牛客过题数</th>
                </tr>
                {table_rows}
            </table>
        </body>
        </html>
        """
        
        return web.Response(
            text=html_content,
            content_type="text/html",
            charset="utf-8"
        )

    def _resolve_qq_id(self, event: AstrMessageEvent) -> int | None:
        sender_id = event.get_sender_id()
        try:
            return int(sender_id)
        except (TypeError, ValueError):
            pass

        # WebChat testing: sender_id may be empty, use session_id hash as a stable numeric id
        if event.get_platform_name() == "webchat":
            session_id = event.get_session_id()
            if not session_id:
                return None
            digest = hashlib.sha256(session_id.encode("utf-8")).hexdigest()
            return int(digest[:15], 16)

        return None

    async def _fetch_cf_accepted_records(
        self, handle: str, max_pages: int = 5
    ) -> Iterable[tuple[str, str, str, str, int]]:
        """Fetch accepted CF submissions for initial sync.

        Returns tuples: (problem_id, problem_name, problem_rating, problem_url, submit_time)
        """
        results: list[tuple[str, str, str, str, int]] = []
        seen: set[str] = set()

        async with aiohttp.ClientSession() as session:
            for page in range(max_pages):
                params = {"handle": handle, "from": str(page * 100 + 1), "count": "100"}
                url = "https://codeforces.com/api/user.status"
                try:
                    async with session.get(url, params=params, timeout=20) as response:
                        response.raise_for_status()
                        data = await response.json()
                except Exception as exc:
                    logger.error(f"CF API 请求失败: {exc}")
                    break

                if data.get("status") != "OK":
                    logger.error(f"CF API 返回异常: {data.get('comment')}")
                    break

                submissions = data.get("result", [])
                if not submissions:
                    break

                for sub in submissions:
                    if sub.get("verdict") != "OK":
                        continue

                    prob = sub.get("problem", {})
                    contest_id = prob.get("contestId")
                    problem_index = prob.get("index")
                    problem_name = prob.get("name") or "Unknown Problem"
                    rating = str(prob.get("rating", ""))
                    submit_time = int(sub.get("creationTimeSeconds", 0))

                    if contest_id and problem_index:
                        problem_id = f"cf_{contest_id}{problem_index}"
                        url_part = (
                            f"gym/{contest_id}/problem/{problem_index}"
                            if contest_id >= 100000
                            else f"problemset/problem/{contest_id}/{problem_index}"
                        )
                        problem_url = f"https://codeforces.com/{url_part}"
                    else:
                        name_norm = "".join(c for c in problem_name if c.isalnum()).lower()
                        problem_id = f"cf_{name_norm}_{rating or 'unknown'}"
                        problem_url = ""

                    if problem_id in seen:
                        continue
                    seen.add(problem_id)
                    results.append(
                        (problem_id, problem_name, rating, problem_url, submit_time)
                    )

                if len(submissions) < 100:
                    break

        return results

    async def _fetch_cf_rating(self, handle: str) -> int:
        url = "https://codeforces.com/api/user.info"
        params = {"handles": handle}
        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(url, params=params, timeout=15) as response:
                    response.raise_for_status()
                    data = await response.json()
                    if data.get("status") == "OK" and data.get("result"):
                        return data["result"][0].get("rating", 0)
            except Exception as e:
                logger.error(f"CF Rating 获取失败 ({handle}): {e}")
        return 0

    async def _fetch_niuke_stats(self, handle: str) -> dict:
        """Fetch stats for a Nowcoder user by scraping html."""
        import re
        async with aiohttp.ClientSession() as session:
            try:
                # 1. 尽可能从搜索页面获取内部数字 ID
                search_url = f"https://ac.nowcoder.com/acm/contest/rating-index?searchUserName={handle}"
                uid = handle
                if not handle.isdigit():
                    async with session.get(search_url, timeout=10) as response:
                        html = await response.text()
                        # 尝试精确匹配用户名对应的 uid
                        match = re.search(rf'href="/acm/contest/profile/(\d+)"[^>]*>\s*<span[^>]*>{re.escape(handle)}</span>', html, re.IGNORECASE)
                        if not match:
                            # 退而求其次取第一个 uid
                            match = re.search(r'data-uid="(\d+)"', html)
                        if match:
                            uid = match.group(1)
                        else:
                            return {} # 未搜索到用户

                # 2. 从用户的刷题主页获取过题数
                profile_url = f"https://ac.nowcoder.com/acm/contest/profile/{uid}/practice-coding"
                async with session.get(profile_url, timeout=10) as response:
                    html = await response.text()
                    # 正则抓取 题已通过
                    match = re.search(r'<div class="state-num">(\d+)</div>\s*<span>题已通过</span>', html)
                    coding_ac = int(match.group(1)) if match else 0
                    
                    return {
                        "name": handle,
                        "uid": uid,
                        "codingAC": coding_ac,
                        "questionAC": 0
                    }
            except Exception as exc:
                logger.error(f"Niuke 网页抓取失败: {exc}")
                return {}

    @filter.command("add")
    async def add_handle(self, event: AstrMessageEvent, platform: str, handle: str):
        """绑定刷题平台账号。例如: /add cf my_cf_handle
        支持的平台: cf (Codeforces), atc (AtCoder), niuke/牛客, luogu/洛谷"""
        platform = platform.lower()
        
        # 平台别名映射，方便用户输入中文
        platform_mapping = {
            "cf": "cf", "codeforces": "cf",
            "atc": "atc", "atcoder": "atc",
            "niuke": "niuke", "nowcoder": "niuke", "牛客": "niuke",
            "luogu": "luogu", "洛谷": "luogu"
        }
        
        if platform not in platform_mapping:
            yield event.plain_result(f"不支持的平台：{platform}。支持的平台有：cf, atc, 牛客, 洛谷")
            return
            
        real_platform = platform_mapping[platform]
            
        # 获取发送者 QQ 号
        qq_id = self._resolve_qq_id(event)
        if qq_id is None:
            yield event.plain_result("无法解析你的 QQ 号，绑定失败。")
            return
            
        try:
            existing_user = self.db.get_user(qq_id)
            # 绑定账号，如果不存在此 QQ 号则会在数据库中自动新建
            self.db.bind_handle(qq_id, real_platform, handle)
            platform_display = {"cf": "Codeforces", "atc": "AtCoder", "niuke": "牛客", "luogu": "洛谷"}
            yield event.plain_result(f"绑定成功！已将你的 {platform_display[real_platform]} 账号绑定为 {handle}。")

            # 绑定 CF 账号时，同步做题记录（每次绑定都尝试同步更新）
            if real_platform == "cf":
                yield event.plain_result("正在同步 CF 数据，请稍候...")
                
                # 同步 Rating
                cf_rating = await self._fetch_cf_rating(handle)
                self.db.update_cf_rating(qq_id, cf_rating)
                
                records = await self._fetch_cf_accepted_records(handle)
                if records:
                    added = self.db.add_solved_records(qq_id, "cf", records)
                    total_cf = self.db.count_solved(qq_id, "cf")
                    yield event.plain_result(f"同步完成，你的最新 CF Rating 为：{cf_rating}\n"
                                             f"本次新同步 {added} 条记录。\n当前系统共记录你的 CF 过题数：{total_cf} 题")
                else:
                    total_cf = self.db.count_solved(qq_id, "cf")
                    yield event.plain_result(f"你的最新 CF Rating 为：{cf_rating}\n"
                                             f"未获取到新的 CF 过题记录，当前共记录 {total_cf} 题。")
                    
            # 绑定牛客账号时，获取并展示信息
            elif real_platform == "niuke":
                yield event.plain_result("正在获取牛客用户信息，请稍候...")
                stats = await self._fetch_niuke_stats(handle)
                if stats and "name" in stats:
                    # 抓取页面取得结果
                    name = stats.get("name", handle)
                    coding_ac = stats.get("codingAC", 0)
                    
                    yield event.plain_result(
                        f"获取成功！\n"
                        f"牛客绑定昵称：{name}\n"
                        f"牛客相关过题数：{coding_ac} 题"
                    )
                else:
                    yield event.plain_result("未获取到牛客信息，可能是接口异常或ID错误，但账号已强制绑定。")
        except Exception as e:
            logger.error(f"绑定账号时发生错误: {e}")
            yield event.plain_result("绑定失败，后台发生错误。")

