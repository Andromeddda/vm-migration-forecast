# VM Migration Forecasting

## Скачать Google 2019 без BigQuery, авторизации и браузера

Для нового HTTP-экстрактора достаточно локального Python. Он собирает 30 дней
историй выбранных инстансов, сохраняет полные истории в Parquet с жёстким
пределом **15 GB = 15 000 000 000 байт**, контролирует рабочее место и память,
возобновляет завершённые шарды после остановки. Google Cloud CLI и bootstrap
для этого способа не нужны.

```sh
uv venv --python 3.11 .venv-http
uv pip install --python .venv-http/bin/python -r requirements-http.txt
screen -S google2019
.venv-http/bin/python tools/download_google2019.py download \
  --output /path/to/data/google2019_http_a_30d \
  --work /path/to/local-scratch/google2019-http
```

В `screen` нажать **Ctrl+A, затем D**, чтобы оставить процесс работать.
Вернуться: `screen -r google2019`. При прерывании повторить **ту же команду
с теми же параметрами и каталогами**. Полная инструкция, ограничения, состав
данных и подготовка среза для ноутбуков: [docs/HTTP_EXTRACTION.md](docs/HTTP_EXTRACTION.md).

**15 GB — предел и цель, а не обещанный объём:** приоритет — 30 дней,
не более 25 000 инстансов и все доступные исследовательские поля. Если этот
набор меньше 15 GB, экстрактор сохраняет его без искусственного раздувания.
Трафик источника значительно больше результата; срок завершения зависит от сети.

## Клонировать проект

```sh
git clone https://github.com/Andromeddda/vm-migration-forecast && cd vm-migration-forecast
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

1. Войти в Google Cloud консоль:
   https://console.cloud.google.com/bigquery

2. Создать проект

4. Скопировать Project ID

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

**Ctrl+C** в терминале.

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
