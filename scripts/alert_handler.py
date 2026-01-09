#!/usr/bin/env python3
"""
Обработчик событий workflow_run.
"""

import os
import sys
import json

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from scripts.alert_sender import send_alert, send_simple_alert
except ImportError:
    # Фоллбек импорт
    from alert_sender import send_alert, send_simple_alert

def main():
    """Точка входа."""
    print("🚴 Обработчик алертов Courier Mules")
    
    event_name = os.environ.get("GITHUB_EVENT_NAME", "")
    print(f"Событие: {event_name}")
    
    # Читаем данные события из файла если есть
    if event_name == "workflow_run":
        # Пробуем прочитать данные события
        github_event_path = os.environ.get("GITHUB_EVENT_PATH")
        
        if github_event_path and os.path.exists(github_event_path):
            try:
                with open(github_event_path, 'r') as f:
                    event_data = json.load(f)
                
                workflow_name = event_data.get("workflow", {}).get("name", "Unknown")
                workflow_run = event_data.get("workflow_run", {})
                status = workflow_run.get("status", "unknown")
                conclusion = workflow_run.get("conclusion", "unknown")
                
                print(f"Workflow: {workflow_name}")
                print(f"Status: {status}")
                print(f"Conclusion: {conclusion}")
                
                # Определяем тип
                if "parse" in workflow_name.lower():
                    ctx = "parser"
                    emoji = "🔍"
                elif "publish" in workflow_name.lower():
                    ctx = "publisher"
                    emoji = "📢"
                else:
                    ctx = "system"
                    emoji = "⚙️"
                
                # Отправляем алерт
                if status == "requested":
                    send_alert(
                        f"{emoji} {workflow_name} запущен",
                        details=f"ID запуска: #{workflow_run.get('run_number', '?')}",
                        alert_type="start",
                        context=ctx
                    )
                elif status == "completed":
                    if conclusion == "success":
                        send_alert(
                            f"{emoji} {workflow_name} успешно завершен",
                            details=f"ID запуска: #{workflow_run.get('run_number', '?')}",
                            alert_type="success",
                            context=ctx
                        )
                    else:
                        send_alert(
                            f"{emoji} {workflow_name} завершен со статусом: {conclusion}",
                            details=f"ID запуска: #{workflow_run.get('run_number', '?')}",
                            alert_type="error" if conclusion == "failure" else "warning",
                            context=ctx
                        )
                else:
                    send_simple_alert(f"{workflow_name} - статус: {status}")
                
            except Exception as e:
                print(f"Ошибка обработки события: {e}")
                send_simple_alert(f"Ошибка обработки события: {e}")
        else:
            print("⚠️ Нет данных события")
            send_simple_alert("⚠️ Нет данных события workflow_run")
    
    elif event_name == "workflow_dispatch":
        send_simple_alert("🔄 Ручной запуск системы алертов")
    
    else:
        send_simple_alert(f"Неподдерживаемое событие: {event_name}")
    
    print("✅ Обработка завершена")

if __name__ == "__main__":
    main()
