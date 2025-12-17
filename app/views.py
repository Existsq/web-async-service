from rest_framework.decorators import api_view, permission_classes
from rest_framework.response import Response
from rest_framework import status
from rest_framework.permissions import AllowAny

import time
import requests

from concurrent import futures

# URL основного сервера
CALLBACK_URL = "http://localhost:8080/api/calculate-cpi/"

# Токен для авторизации (8 байт)
AUTH_TOKEN = "lab8token"

# Пул потоков для асинхронных задач
executor = futures.ThreadPoolExecutor(max_workers=1)


def calculate_personal_cpi(request_id):
    """
    Асинхронная функция для расчета персонального ИПЦ для заявки.
    Имитирует долгую задачу (30 секунд) и возвращает результат расчета.
    
    Формула: ИПЦ_перс = Σ_i w_i(t1) * (P_i(t1) - P_i(t0)) / P_i(t0)
    где w_i = userSpent / totalSpent (вес категории)
    P_i(t1) = userSpent (текущая цена)
    P_i(t0) = basePrice (базовая цена на дату comparisonDate)
    """
    # Задержка 30 секунд (имитация долгой задачи)
    delay = 30
    time.sleep(delay)
    
    # Получаем информацию о заявке для расчета ИПЦ
    try:
        url = f"{CALLBACK_URL}{request_id}/async-data"
        print(f"Fetching request data for request {request_id} from {url}")
        response = requests.get(
            url,
            timeout=10,
            headers={
                "Content-Type": "application/json",
                "X-Auth-Token": AUTH_TOKEN
            }
        )
        print(f"Response status: {response.status_code}")
        
        if response.status_code == 200:
            request_data = response.json()
            categories = request_data.get('categories', [])
            comparison_date = request_data.get('comparisonDate')
            
            print(f"Request {request_id}: Found {len(categories)} categories, comparisonDate: {comparison_date}")
            
            if not categories:
                print(f"No categories found for request {request_id}")
                return {
                    "id": request_id,
                    "personalCPI": None,
                    "success": False
                }
            
            # Рассчитываем персональный ИПЦ
            total_spent = sum(cat.get('userSpent', 0) or 0 for cat in categories)
            
            print(f"Request {request_id}: Total spent = {total_spent}")
            
            if total_spent == 0:
                print(f"Total spent is 0 for request {request_id}")
                return {
                    "id": request_id,
                    "personalCPI": None,
                    "success": False
                }
            
            # Рассчитываем ИПЦ по формуле
            # Формула: ИПЦ_перс = Σ_i w_i * (P_i(t1) - P_i(t0)) / P_i(t0)
            # где w_i = userSpent / totalSpent (вес категории)
            # P_i(t1) = userSpent (текущая цена)
            # P_i(t0) = basePrice (базовая цена на дату comparisonDate)
            personal_cpi = 0.0
            has_valid_categories = False
            
            for category in categories:
                user_spent = category.get('userSpent', 0) or 0
                base_price = category.get('basePrice', 0) or 0
                
                if base_price > 0 and user_spent > 0:
                    weight = user_spent / total_spent
                    change = (user_spent - base_price) / base_price
                    personal_cpi += weight * change
                    has_valid_categories = True
                    print(f"Category {category.get('id')}: weight={weight:.4f}, change={change:.4f}")
            
            if not has_valid_categories:
                print(f"No valid categories with basePrice > 0 for request {request_id}")
                return {
                    "id": request_id,
                    "personalCPI": None,
                    "success": False
                }
            
            # Округляем до 2 знаков после запятой и преобразуем в проценты
            personal_cpi = round(personal_cpi * 100, 2)
            print(f"Request {request_id}: Calculated personalCPI = {personal_cpi}%")
            
            return {
                "id": request_id,
                "personalCPI": personal_cpi,
                "success": True
            }
        else:
            print(f"Failed to fetch request {request_id}: HTTP {response.status_code}")
            print(f"Response text: {response.text}")
            return {
                "id": request_id,
                "personalCPI": None,
                "success": False
            }
    except requests.exceptions.RequestException as e:
        print(f"Request error fetching request data for {request_id}: {e}")
        import traceback
        traceback.print_exc()
    except Exception as e:
        print(f"Error fetching request data for {request_id}: {e}")
        import traceback
        traceback.print_exc()
    
    # Если не удалось получить данные, возвращаем неуспех
    return {
        "id": request_id,
        "personalCPI": None,
        "success": False
    }


def result_callback(task):
    """
    Callback функция, которая вызывается после завершения асинхронной задачи.
    Отправляет результаты расчета ИПЦ на основной сервер.
    """
    try:
        result = task.result()
        print(f"Task completed for request {result['id']}")
        print(f"Personal CPI: {result.get('personalCPI')}, Success: {result.get('success')}")
    except futures._base.CancelledError:
        return
    except Exception as e:
        print(f"Error in task: {e}")
        import traceback
        traceback.print_exc()
        return
    
    # Отправляем результаты на основной сервер
    try:
        request_id = result['id']
        url = f"{CALLBACK_URL}{request_id}/async-result"
        
        # Формируем данные для отправки
        payload = {
            "personalCPI": result.get('personalCPI'),
            "success": result.get('success', False)
        }
        
        headers = {
            "Content-Type": "application/json",
            "X-Auth-Token": AUTH_TOKEN
        }
        
        response = requests.put(url, json=payload, headers=headers, timeout=10)
        
        if response.status_code == 200:
            print(f"Successfully sent personal CPI results for request {request_id}")
        else:
            print(f"Failed to send results: {response.status_code} - {response.text}")
    except Exception as e:
        print(f"Error sending results: {e}")
        import traceback
        traceback.print_exc()


@api_view(['POST', 'OPTIONS'])
@permission_classes([AllowAny])
def process_request(request):
    """
    Обработчик POST запроса для запуска асинхронной обработки заявки.
    Принимает pk (ID заявки) и token для авторизации.
    """
    # Обработка OPTIONS запроса для CORS
    if request.method == 'OPTIONS':
        response = Response(status=status.HTTP_200_OK)
        response['Access-Control-Allow-Origin'] = '*'
        response['Access-Control-Allow-Methods'] = 'POST, OPTIONS'
        response['Access-Control-Allow-Headers'] = 'Content-Type, X-Auth-Token, Authorization'
        response['Access-Control-Max-Age'] = '86400'
        return response
    
    try:
        # Логируем входящий запрос для отладки
        print(f"Received request: {request.data}")
        print(f"Request method: {request.method}")
        print(f"Content-Type: {request.content_type}")
        
        # Проверяем наличие обязательных полей
        if not request.data:
            print("Warning: Received empty request body")
            return Response(
                {"error": "Request body is empty"},
                status=status.HTTP_400_BAD_REQUEST
            )
        
        if "pk" not in request.data.keys() or "token" not in request.data.keys():
            return Response(
                {"error": "Missing required fields: pk and token"},
                status=status.HTTP_400_BAD_REQUEST
            )
        
        request_id = request.data["pk"]
        token = request.data["token"]
        
        # Проверяем токен
        if token != AUTH_TOKEN:
            return Response(
                {"error": "Invalid token"},
                status=status.HTTP_401_UNAUTHORIZED
            )
        
        # Запускаем асинхронную задачу для расчета ИПЦ
        task = executor.submit(calculate_personal_cpi, request_id)
        task.add_done_callback(result_callback)
        
        print(f"Async task started for request {request_id}")
        
        response = Response(
            {"message": f"Processing started for request {request_id}"},
            status=status.HTTP_200_OK
        )
        # Добавляем CORS заголовки
        response['Access-Control-Allow-Origin'] = '*'
        response['Access-Control-Allow-Methods'] = 'POST, OPTIONS'
        response['Access-Control-Allow-Headers'] = 'Content-Type, X-Auth-Token, Authorization'
        return response
    except Exception as e:
        print(f"Error in process_request: {e}")
        import traceback
        traceback.print_exc()
        return Response(
            {"error": f"Internal server error: {str(e)}"},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR
        )
