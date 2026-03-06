async def subscription_check_loop(session: aiohttp.ClientSession, store: Dict[str, Any]):
    print("✅ Subscription check loop started")
    while True:
        await asyncio.sleep(SUBSCRIPTION_CHECK_INTERVAL_SEC)
        try:
            subs = all_subs(store)
            for chat_id in subs:
                # Администраторы никогда не отписываются автоматически
                if chat_id in ADMIN_IDS:
                    continue
                meta = get_sub_meta(store, chat_id)
                if meta.get("chat_type") != "private":
                    continue
                active = await check_site_subscription(session, chat_id)
                if not active:
                    unsubscribe(store, chat_id)
                    print(f"Subscription expired for chat_id={chat_id}, unsubscribed.")
                    try:
                        await tg_send(session, chat_id,
                                      "❌ Ваша подписка закончилась. Доступ к сигналам приостановлен.\n"
                                      "Для возобновления обратитесь к администратору на сайте.")
                    except Exception:
                        pass
        except Exception as e:
            print(f"Subscription check loop error: {type(e).__name__}: {e}")