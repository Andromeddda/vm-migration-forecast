# Быстрый запуск

Сначала выполните разделы 1–3 [README](../README.md): standalone uv, Google Cloud CLI
и Google Cloud project для реальной выгрузки. Для синтетики Google Cloud не нужен.

```sh
sh lab configure
sh lab auth
sh lab bootstrap
```

В синтетическом режиме пропустите `auth`, оставив project пустым при configure.
Откройте напечатанную Jupyter-ссылку на localhost:8888 с токеном. В notebooks
откройте 02 или 03 и выполните Run All Cells. Остановка — Ctrl+C в терминале.

Все зависимости и нужный Python устанавливает bootstrap, системный Python
не используется. Повторный bootstrap сохраняет данные и готовое окружение.
