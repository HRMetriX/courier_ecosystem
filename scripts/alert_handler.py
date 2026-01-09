#!/usr/bin/env python3
"""
Обработчик событий workflow_run.
Анализирует события от GitHub Actions и отправляет соответствующие алерты.
"""

import os
import json
import sys
from datetime import datetime
from typing import Dict, Any, Optional

# Добавляем путь для импорта alert_sender
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from scripts.alert_sender import send_alert, send_simple_alert
except ImportError:
    # Fallback если не удалось импортировать
    import requests
    
    def send_simple_alert_fallback(text: str) -> bool:
        """Упрощенная отправка алерта (fallback)."""
        bot_token = os.environ.get("TG_ALERT_BOT_TOKEN")
        chat_id = os.environ.get("TG_ALERT_CHAT_ID")
        
        if not bot_token or not chat_id:
            print(f"⚠️ Не хватает переменных окружения для отправки: {text}")
            return False
            
        try:
            url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
            payload = {
                "chat_id": chat_id,
                "text": f"🚴 Courier Mules\n{text}",
                "parse_mode": "HTML"
            }
            response = requests.post(url, json=payload, timeout=10)
            return response.status_code == 200
        except:
            return False
    
    send_simple_alert = send_simple_alert_fallback

def parse_github_context() -> Dict[str, Any]:
    """Парсит контекст GitHub из переменных окружения."""
    try:
        github_context = json.loads(os.environ.get("GITHUB_CONTEXT", "{}"))
        event_context = json.loads(os.environ.get("EVENT_CONTEXT", "{}"))
        return {
            "github": github_context,
            "event": event_context
        }
    except:
        return {}

def get_workflow_info(context: Dict[str, Any]) -> Dict[str, str]:
    """Извлекает информацию о workflow из контекста."""
    event = context.get("event", {})
    workflow_run = event.get("workflow_run", {})
    workflow = event.get("workflow", {})
    
    return {
        "workflow_name": workflow.get("name", "Unknown"),
        "run_id": str(workflow_run.get("id", "")),
        "run_number": str(workflow_run.get("run_number", "")),
        "status": workflow_run.get("status", "unknown"),
        "conclusion": workflow_run.get("conclusion", "unknown"),
        "actor": workflow_run.get("actor", {}).get("login", "unknown"),
        "trigger": workflow_run.get("event", "unknown"),
        "html_url": workflow_run.get("html_url", ""),
        "created_at": workflow_run.get("created_at", ""),
        "updated_at": workflow_run.get("updated_at", ""),
    }

def format_duration(start: str, end: str) -> str:
    """Форматирует длительность выполнения."""
    try:
        fmt = "%Y-%m-%dT%H:%M:%SZ"
        start_dt = datetime.strptime(start, fmt)
        end_dt = datetime.strptime(end, fmt)
        duration = end_dt - start_dt
        
        hours, remainder = divmod(duration.total_seconds(), 3600)
        minutes, seconds = divmod(remainder, 60)
        
        if hours > 0:
            return f"{int(hours)}ч {int(minutes)}м"
        elif minutes > 0:
            return f"{int(minutes)}м {int(seconds)}с"
        else:
            return f"{int(seconds)}с"
    except:
        return "unknown"

def handle_workflow_event():
    """Основная функция обработки события workflow."""
    context = parse_github_context()
    info = get_workflow_info(context)
    
    workflow_name = info["workflow_name"]
    status = info["status"]
    conclusion = info["conclusion"]
    run_id = info["run_id"]
    actor = info["actor"]
    trigger = info["trigger"]
    
    # Определяем тип workflow
    if "parse" in workflow_name.lower() or "parser" in workflow_name.lower():
        context_type = "parser"
        emoji = "🔍"
    elif "publish" in workflow_name.lower():
        context_type = "publisher"
        emoji = "📢"
    else:
        context_type = "system"
        emoji = "⚙️"
    
    # Обрабатываем разные статусы
    if status == "requested":
        # Workflow запущен
        send_alert(
            f"{emoji} {workflow_name} запущен",
            details=f"Инициатор: {actor}\nТриггер: {trigger}\nRun ID: #{info['run_number']}",
            alert_type="start",
            context=context_type
        )
        
    elif status == "completed":
        # Workflow завершен
        duration = format_duration(info["created_at"], info["updated_at"])
        
        if conclusion == "success":
            # Успешное завершение
            stats = {
                "run_id": f"#{info['run_number']}",
                "duration": duration,
                "trigger": trigger,
                "actor": actor,
            }
            
            send_alert(
                f"{emoji} {workflow_name} завершен успешно",
                details=f"Все этапы выполнены без ошибок",
                stats=stats,
                alert_type="success",
                context=context_type
            )
            
        elif conclusion == "failure":
            # Ошибка
            send_alert(
                f"{emoji} {workflow_name} завершен с ошибкой",
                details=f"Run ID: #{info['run_number']}\nДлительность: {duration}\nИнициатор: {actor}",
                alert_type="error",
                context=context_type
            )
            
        elif conclusion == "cancelled":
            # Отменен
            send_alert(
                f"{emoji} {workflow_name} отменен",
                details=f"Run ID: #{info['run_number']}\nИнициатор: {actor}",
                alert_type="warning",
                context=context_type
            )
            
        else:
            # Другие статусы
            send_alert(
                f"{emoji} {workflow_name} завершен со статусом: {conclusion}",
                details=f"Run ID: #{info['run_number']}\nДлительность: {duration}",
                alert_type="info",
                context=context_type
            )
    
    elif status == "in_progress":
        # В процессе (опционально, можно отключить)
        pass
    
    else:
        # Неизвестный статус
        send_simple_alert(f"{workflow_name} - неизвестный статус: {status}")

def main():
    """Точка входа."""
    print("🚴 Обработчик алертов Courier Mules")
    print(f"Время: {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}")
    
    # Проверяем тип события
    event_name = os.environ.get("GITHUB_EVENT_NAME", "")
    print(f"Событие: {event_name}")
    
    if event_name == "workflow_run":
        handle_workflow_event()
    else:
        print(f"⚠️ Неподдерживаемое событие: {event_name}")
        send_simple_alert(f"Неподдерживаемое событие: {event_name}")
    
    print("✅ Обработка завершена")

if __name__ == "__main__":
    main()
