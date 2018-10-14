## Memc Load (Golang)

Скрипт загрузки файлов в формате protobuf в Memcached. Задача была создать мультипоточную версию скрипта на Go и сравнить ее с аналогичным скриптом на Python.

Используется Golang 1.11

### Параметры запуска:

* dry - тестовый запуск обработки
* pattern - шаблон имени файла для обработки
* workers - количество воркеров
* test - режим тестирования данных
* log - файл логов
* idfa, gaid, adid, dvid - адреса серверов Memcached для данных соответствующего типа

### Пример запуска

Запуск обработки файлов с 500 воркерами

    go build
    memcload --pattern=./memcload/*.tsv.gz --workers=500  --log=log_1.log

### Результаты тестирования

Для тестов использовалась одна VM в качестве клиента (1 Core, 4 Gb RAM) и две VM Memcached (1 Core, 4 Gb RAM).
Количество воркеров в каждом случае подобрано исходя из максимально возможной утилизации указанных ресурсов, с целью выяснить наименьшее время выполнения задания в обоих случаях.
Обработка проводилась на двух файлах, примерно по 3,4 млн записей в каждом.
При этом нагрузка на CPU на машине клиента составила около 90%, на машинах Memcached - около 40%.

Python: число воркеров - 200, время выполнения задания - 90 минут

Golang: число воркеров - 500, время выполнения задания - 13 минут

