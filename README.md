# VM Migration Forecasting

## Клонировать проект

```sh
git clone https://github.com/Andromeddda/vm-migration-forecast
cd vm-migration-forecast
```

## Проверить требования

- Linux x86_64 или aarch64 с glibc
- uv >= 0.9.5
- Для watchdog: Bash >= 4.4, systemd >= 243 и screen

## Установить uv

```sh
curl -LsSf https://astral.sh/uv/install.sh | sh
```

Открыть новый терминал:

```sh
uv --version
```

## Настроить локальные каталоги

```sh
sh lab configure
```

При необходимости выбрать другие непересекающиеся каталоги вне исходников:

```sh
sh lab configure \
  --state-dir /path/to/local-state \
  --data-dir /path/to/data \
  --runs-dir /path/to/runs
```

## Запустить Jupyter

```sh
sh lab bootstrap
```

Открыть напечатанную ссылку на `http://localhost:8888` с токеном.

1. Открыть папку **notebooks**.
2. Выбрать `02_single_experiment.ipynb` или `03_benchmark_and_reports.ipynb`.
3. Выбрать kernel **VM Migration (local)**.
4. Нажать **Run → Run All Cells**.

Для остановки нажать **Ctrl+C** в терминале сервера.

## Скачать Google Cluster 2019 по HTTP

[Подготовить отдельное окружение и запустить загрузку](docs/HTTP_EXTRACTION.md).
[Продолжить загрузку на двух компьютерах и объединить прогресс](docs/HTTP_PEER.md).

## Подготовить срез скачанных данных

```sh
sh lab prepare
sh lab run -- python tools/download_google2019.py verify \
  --dataset /path/to/data/google2019_http_a_30d
sh lab run -- python tools/download_google2019.py prepare \
  --dataset /path/to/data/google2019_http_a_30d \
  --destination /path/to/data/google2019_http_a_30d_backtest/prepared \
  --objects 64 --hours 168
```

В ноутбуке 02 установить `USE_PREPARED = True` и указать `PREPARED_DIR`,
совпадающий с `--destination`. Для синтетики оставить `USE_PREPARED = False`.

## Запустить команду в фоне с перезапуском

```sh
sudo apt install screen
loginctl enable-linger "$USER"
./tools/watchdog.sh --name my-job -- /path/to/program argument
./tools/watchdog.sh status my-job
./tools/watchdog.sh attach my-job
```

Отсоединиться от screen: **Ctrl+A, затем D**.

```sh
./tools/watchdog.sh stop my-job
./tools/watchdog.sh restart my-job
```

[Настроить надзор и лимит памяти](docs/WATCHDOG.md).

## Проверить проект

```sh
sh lab doctor
sh lab test
sh lab verify-notebooks
sh lab audit
```

## Очистить вывод ноутбуков перед коммитом

```sh
sh lab clean-notebooks
```

## Проверить обновление окружения

```sh
sh lab configure
sh lab prepare
```

[Посмотреть состав очистки и результаты проверок](docs/CLEANUP_VALIDATION.md).
