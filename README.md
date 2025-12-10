# TSBD
Технологии хранения больших данных

P.S. В корне проекта нужно создать файл ".env" и прописать туда переменную CI_REGISTRY_IMAGE=custom_name (вместо custom_name указать своё название).

___


Проверка баз данных в MongoDB:
```bash
mongosh -u admin -p pass --authenticationDatabase admin default
show collections
db.mycollection.find().pretty()
```