# VM Migration Forecasting

## Клонировать проект

```sh
git clone <URL> && cd <PROJECT_DIR>
```

## Проверить требования

- Linux x86_64 или aarch64 с glibc
- uv >= 0.9.5
- gcloud >= 593.0.0

### Установить uv

```sh
curl -LsSf https://astral.sh/uv/install.sh | sh
```

Открыть новый терминал и проверить установку:

```sh
uv --version
```

### Установить gcloud

Скачать с [официального сайта](https://docs.cloud.google.com/sdk/docs/install-sdk).

Или выполнить:

```sh
sh lab install-gcloud
```

## Настроить BigQuery Sandbox

1. Войти в Google Account:
   https://console.cloud.google.com/bigquery
2. Принять условия:
   выбрать страну → отметить **Terms of Service** → **Agree and continue**.
3. Создать проект:
   **Create project** → заполнить **Project name** → **Create**.
   Для существующего аккаунта: селектор проекта сверху → **New project**.
4. Выбрать проект:
   селектор проекта сверху → выбрать созданный проект → проверить надпись **Sandbox**.
5. Скопировать Project ID:
   селектор проекта сверху → столбец **ID**.

- Не привязывать платёжный аккаунт или карту.
- Не включать Free Trial и не нажимать Upgrade.
- Не проходить подтверждение по номеру телефона.
- При запросе карты или телефона остановить настройку.
- Работать в пределах 1 ТиБ запросов в месяц.

## Сохранить настройки

```sh
sh lab configure --project YOUR_PROJECT_ID
```

### Выбрать каталоги данных и результатов

```sh
sh lab configure --data-dir /path/to/data --runs-dir /path/to/runs
```

### Указать gcloud вне PATH

```sh
sh lab configure --gcloud /path/to/gcloud
```

## Авторизоваться

```sh
sh lab auth
```

В обоих окнах Google выбрать один аккаунт и разрешить доступ.

## Запустить Jupyter

```sh
sh lab bootstrap
```

Открыть напечатанную ссылку на `http://localhost:8888` с токеном.

## Выполнить ноутбук

1. Открыть папку **notebooks**.
2. Выбрать ноутбук:
   - `01_extract_google2019.ipynb` — выгрузка Google Cluster 2019;
   - `02_single_experiment.ipynb` — одиночный эксперимент;
   - `03_benchmark_and_reports.ipynb` — серия экспериментов.
3. Выбрать kernel **VM Migration (local)**.
4. Нажать **Run → Run All Cells**.

Для выгрузки в ноутбуке 01 сначала включить `CONNECT_GOOGLE`, проверить dry run,
затем включить `EXECUTE_QUERIES`.

## Запустить синтетику без Google Cloud

```sh
sh lab configure --project ""
sh lab bootstrap
```

Открыть ноутбук 02 или 03.

## Остановить Jupyter

Нажать **Ctrl+C** в терминале.

## Проверить проект

```sh
sh lab doctor
sh lab test
sh lab verify-notebooks
```

## Подготовить исходники к передаче

```sh
sh lab clean-notebooks
sh lab audit
```

## Открыть документацию

- [Выгрузить данные](docs/DATA_ACCESS.md)
- [Прочитать правила проекта](docs/HANDBOOK.md)
- [Посмотреть архитектуру](docs/ARCHITECTURE.md)
- [Посмотреть результаты проверок](docs/VALIDATION.md)
