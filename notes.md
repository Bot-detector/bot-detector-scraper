## setup windows
```
python -m venv .venv
.venv\Scripts\activate
python -m pip install --upgrade pip
pip install -r requirements.txt
```
## setup linux
```
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -r requirements.txt
```
# for admin purposes saving & upgrading

```
venv\Scripts\activate
call pip freeze > requirements.txt
powershell "(Get-Content requirements.txt) | ForEach-Object { $_ -replace '==', '>=' } | Set-Content requirements.txt"
call pip install -r requirements.txt --upgrade
call pip freeze > requirements.txt
powershell "(Get-Content requirements.txt) | ForEach-Object { $_ -replace '>=', '==' } | Set-Content requirements.txt"
```

```
kubectl port-forward -n kafka svc/kafka-service 9092:9092
kubectl port-forward -n kafka svc/bd-prd-kafka-service 9094:9094
kubectl port-forward -n kafka svc/bd-prd-kafdrop-service 9000:9000
kubectl port-forward -n database svc/mysql 3306:3306
```