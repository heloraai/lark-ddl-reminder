import os
import sys
import json
import requests
from datetime import datetime, timezone, timedelta

# ==================== Configuration ====================
LARK_APP_ID = os.environ["LARK_APP_ID"]
LARK_APP_SECRET = os.environ["LARK_APP_SECRET"]
LARK_CHAT_ID = os.environ["LARK_CHAT_ID"]
LARK_BASE_APP_TOKEN = os.environ["LARK_BASE_APP_TOKEN"]
LARK_TABLE_ID = os.environ["LARK_TABLE_ID"]

# Lark Suite API base URL
BASE_URL = "https://open.larksuite.com/open-apis"

# Beijing Time
BJT = timezone(timedelta(hours=8))


def get_tenant_access_token():
    url = f"{BASE_URL}/auth/v3/tenant_access_token/internal"
    payload = {
        "app_id": LARK_APP_ID,
        "app_secret": LARK_APP_SECRET,
    }
    resp = requests.post(url, json=payload)
    resp.raise_for_status()
    data = resp.json()
    if data.get("code") != 0:
        raise Exception(f"Failed to get token: {data}")
    return data["tenant_access_token"]


def get_bitable_records(token):
    url = f"{BASE_URL}/bitable/v1/apps/{LARK_BASE_APP_TOKEN}/tables/{LARK_TABLE_ID}/records"
    headers = {"Authorization": f"Bearer {token}"}
    all_records = []
    page_token = None

    while True:
        params = {"page_size": 100}
        if page_token:
            params["page_token"] = page_token
        resp = requests.get(url, headers=headers, params=params)
        resp.raise_for_status()
        data = resp.json()
        if data.get("code") != 0:
            raise Exception(f"Failed to get records: {data}")
        items = data.get("data", {}).get("items", [])
        all_records.extend(items)
        if not data["data"].get("has_more"):
            break
        page_token = data["data"].get("page_token")

    return all_records


def find_person_field_key(fields):
    for key in fields:
        if "落实人" in key or "Person" in key:
            return key
    return None


def filter_pending_tasks(records):
    today = datetime.now(BJT).date()
    pending = []

    for record in records:
        fields = record.get("fields", {})

        # Check completion status
        is_done = fields.get("完成情况", False)
        if is_done:
            continue

        # Get DDL
        ddl_value = fields.get("DDL")
        if ddl_value is None:
            continue

        if isinstance(ddl_value, (int, float)):
            ddl_date = datetime.fromtimestamp(ddl_value / 1000, tz=BJT).date()
        else:
            try:
                ddl_date = datetime.strptime(str(ddl_value).replace("/", "-"), "%Y-%m-%d").date()
            except ValueError:
                continue

        if ddl_date <= today:
            task_name = fields.get("任务", "未命名任务")

            person_key = find_person_field_key(fields)
            persons = fields.get(person_key, []) if person_key else []

            notes = fields.get("备注", "")

            person_list = []
            if isinstance(persons, list):
                for p in persons:
                    if isinstance(p, dict):
                        person_list.append({
                            "id": p.get("id", ""),
                            "name": p.get("name", "未知"),
                        })

            days_overdue = (today - ddl_date).days

            pending.append({
                "task": task_name,
                "persons": person_list,
                "ddl": str(ddl_date),
                "days_overdue": days_overdue,
                "notes": notes,
            })

    return pending


def build_message_content(pending_tasks):
    if not pending_tasks:
        return None

    person_tasks = {}
    for task in pending_tasks:
        for person in task["persons"]:
            pid = person["id"]
            if pid not in person_tasks:
                person_tasks[pid] = {
                    "name": person["name"],
                    "id": pid,
                    "tasks": [],
                }
            person_tasks[pid]["tasks"].append(task)

        if not task["persons"]:
            if "__unassigned__" not in person_tasks:
                person_tasks["__unassigned__"] = {
                    "name": "未分配",
                    "id": None,
                    "tasks": [],
                }
            person_tasks["__unassigned__"]["tasks"].append(task)

    content = []

    for pid, info in person_tasks.items():
        person_line = []
        if info["id"]:
            person_line.append({"tag": "at", "user_id": info["id"]})
            person_line.append({"tag": "text", "text": f" 你有 {len(info['tasks'])} 项任务需要跟进："})
        else:
            person_line.append({"tag": "text", "text": "⚠️ 以下任务尚未分配落实人："})
        content.append(person_line)

        for i, task in enumerate(info["tasks"], 1):
            task_line = []
            if task["days_overdue"] > 0:
                task_line.append({
                    "tag": "text",
                    "text": f"  {i}. 🔴 【已逾期 {task['days_overdue']} 天】{task['task']}\n     DDL: {task['ddl']}"
                })
            else:
                task_line.append({
                    "tag": "text",
                    "text": f"  {i}. 🟡 【今日截止】{task['task']}\n     DDL: {task['ddl']}"
                })
            if task.get("notes"):
                task_line.append({"tag": "text", "text": f"\n     📝 备注: {task['notes']}"})
            content.append(task_line)

        content.append([{"tag": "text", "text": "\n"}])

    return content


def send_group_message(token, content):
    url = f"{BASE_URL}/im/v1/messages"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    params = {"receive_id_type": "chat_id"}

    msg_body = {
        "receive_id": LARK_CHAT_ID,
        "msg_type": "post",
        "content": json.dumps({
            "zh_cn": {
                "title": "📋 CF进度管理 - DDL提醒",
                "content": content,
            }
        }),
    }

    resp = requests.post(url, headers=headers, params=params, json=msg_body)
    resp.raise_for_status()
    data = resp.json()

    if data.get("code") != 0:
        raise Exception(f"Failed to send message: {data}")

    print(f"Message sent successfully! Message ID: {data['data']['message_id']}")
    return data


def main():
    print("Starting DDL Reminder...")
    print(f"Current time (BJT): {datetime.now(BJT).strftime('%Y-%m-%d %H:%M:%S')}")

    print("Getting tenant access token...")
    token = get_tenant_access_token()

    print("Reading Bitable records...")
    records = get_bitable_records(token)
    print(f"Found {len(records)} total records")

    print("Filtering pending tasks...")
    pending_tasks = filter_pending_tasks(records)
    print(f"Found {len(pending_tasks)} pending/overdue tasks")

    if not pending_tasks:
        print("No pending tasks! Everyone is on track.")
        return

    print("Building message...")
    content = build_message_content(pending_tasks)

    print("Sending group message...")
    send_group_message(token, content)

    print("DDL Reminder completed!")


if __name__ == "__main__":
    main()
