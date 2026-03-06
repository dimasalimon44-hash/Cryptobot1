                    else:
                        # Администратор подписывается без проверки сайта
                        if is_admin:
                            subscribe(store, chat_id, chat_type=chat_type, is_forum=is_forum)
                            await tg_send(session, chat_id,
                                          "✅ Подписка включена (администратор).
Помощь: /help")
                        else:
                            approved = await check_site_subscription(session, chat_id)
                            if approved:
                                subscribe(store, chat_id, chat_type=chat_type, is_forum=is_forum)
                                await tg_send(session, chat_id,
                                              "✅ Подписка активна! Буду присылать сигналы.
Помощь: /help")
                            else:
                                await tg_send(session, chat_id,
                                              "❌ Подписка не найдена или не активна.\n"
                                              "Зарегистрируйтесь на сайте и нажмите «Привязать Telegram»."  
