from fastapi import FastAPI
from pydantic import BaseModel
import psycopg2
import json
from typing import Optional
from datetime import datetime, time

app = FastAPI(title="Gaming Twin Backend")

# ---------- MODELS ----------

class GamingEvent(BaseModel):
    user_id: str
    game_name: str
    duration: int  # minutes


class ThresholdUpdate(BaseModel):
    daily: Optional[int] = None
    night: Optional[int] = None


# ---------- DB CONNECTION ----------

def get_db_connection():
    """Connect to PostgreSQL gaming_twin_db."""
    return psycopg2.connect(
        host="localhost",
        database="gaming_twin_db",
        user="postgres",
        password="2404"
    )


# ---------- ROUTES ----------

@app.get("/health")
async def health():
    """Check API + DB status."""
    try:
        conn = get_db_connection()
        conn.close()
        return {"status": "healthy", "database": "gaming_twin_db connected"}
    except Exception as e:
        return {"status": "error", "message": str(e)}


@app.post("/events")
async def ingest_event(event: GamingEvent):
    """
    1) Store raw event in events table
    2) Ensure digital twin row exists
    3) Update aggregates: today_minutes, weekly_minutes, night_minutes
    4) Update state (Healthy / Moderate / Excessive)
    """
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        # 1) Insert raw event
        cur.execute(
            """
            INSERT INTO events (user_id, game_name, duration)
            VALUES (%s, %s, %s)
            """,
            (event.user_id, event.game_name, event.duration)
        )

        # 2) Make sure a twin row exists
        cur.execute(
            """
            INSERT INTO digital_twins (user_id)
            VALUES (%s)
            ON CONFLICT (user_id) DO NOTHING
            """,
            (event.user_id,)
        )

        # 3) Read current aggregates
        cur.execute(
            "SELECT aggregates FROM digital_twins WHERE user_id = %s",
            (event.user_id,)
        )
        row = cur.fetchone()
        aggregates: Optional[dict] = row[0] if row and row[0] is not None else None

        if not aggregates:
            aggregates = {
                "today_minutes": 0,
                "weekly_minutes": 0,
                "night_minutes": 0
            }

        # 4a) Update today_minutes
        new_today = int(aggregates.get("today_minutes", 0)) + int(event.duration)
        aggregates["today_minutes"] = new_today

        # 4b) Update weekly_minutes (simple cumulative)
        new_weekly = int(aggregates.get("weekly_minutes", 0)) + int(event.duration)
        aggregates["weekly_minutes"] = new_weekly

        # 4c) Update night_minutes if in night window (22:00–06:00)
        now = datetime.now().time()
        if now >= time(22, 0) or now <= time(6, 0):
            new_night = int(aggregates.get("night_minutes", 0)) + int(event.duration)
            aggregates["night_minutes"] = new_night

        # 5) Decide new state based on today_minutes
        if new_today > 120:
            new_state = "Excessive"
        elif new_today > 60:
            new_state = "Moderate"
        else:
            new_state = "Healthy"

        # 6) Write back aggregates AND state
        cur.execute(
            """
            UPDATE digital_twins
            SET aggregates = %s,
                state = %s,
                updated_at = NOW()
            WHERE user_id = %s
            """,
            (json.dumps(aggregates), new_state, event.user_id)
        )

        conn.commit()
        cur.close()
        conn.close()

        return {
            "status": "processed",
            "user_id": event.user_id,
            "game": event.game_name,
            "today_minutes": new_today,
            "weekly_minutes": new_weekly,
            "state": new_state
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}


@app.get("/digital-twin/{user_id}")
async def get_twin(user_id: str):
    """Return the digital twin row for a user."""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT user_id, thresholds, aggregates, state
            FROM digital_twins
            WHERE user_id = %s
            """,
            (user_id,)
        )
        result = cur.fetchone()
        cur.close()
        conn.close()

        if result:
            return {
                "user_id": result[0],
                "thresholds": result[1],
                "aggregates": result[2],
                "state": result[3]
            }
        return {"error": "User not found"}
    except Exception as e:
        return {"status": "error", "message": str(e)}


@app.get("/reports/{user_id}")
async def get_report(user_id: str):
    """
    Simple report for dashboard:
    - today_minutes, weekly_minutes, night_minutes
    - state and thresholds
    """
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT thresholds, aggregates, state
            FROM digital_twins
            WHERE user_id = %s
            """,
            (user_id,)
        )
        row = cur.fetchone()
        cur.close()
        conn.close()

        if not row:
            return {"error": "User not found"}

        thresholds = row[0] or {}
        aggregates = row[1] or {}
        state = row[2]

        return {
            "user_id": user_id,
            "today_minutes": aggregates.get("today_minutes", 0),
            "weekly_minutes": aggregates.get("weekly_minutes", 0),
            "night_minutes": aggregates.get("night_minutes", 0),
            "state": state,
            "daily_threshold": thresholds.get("daily", 120),
            "night_threshold": thresholds.get("night", 60)
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}


@app.post("/digital-twin/{user_id}/threshold")
async def update_threshold(user_id: str, body: ThresholdUpdate):
    """Update daily/night thresholds for a user's twin (for parent settings)."""
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        # get existing thresholds
        cur.execute(
            "SELECT thresholds FROM digital_twins WHERE user_id = %s",
            (user_id,)
        )
        row = cur.fetchone()
        if not row:
            # create twin if not exists
            cur.execute(
                """
                INSERT INTO digital_twins (user_id)
                VALUES (%s)
                ON CONFLICT (user_id) DO NOTHING
                """,
                (user_id,)
            )
            thresholds = {"daily": 120, "night": 60}
        else:
            thresholds = row[0] or {"daily": 120, "night": 60}

        if body.daily is not None:
            thresholds["daily"] = body.daily
        if body.night is not None:
            thresholds["night"] = body.night

        cur.execute(
            """
            UPDATE digital_twins
            SET thresholds = %s, updated_at = NOW()
            WHERE user_id = %s
            """,
            (json.dumps(thresholds), user_id)
        )
        conn.commit()
        cur.close()
        conn.close()

        return {"user_id": user_id, "thresholds": thresholds}
    except Exception as e:
        return {"status": "error", "message": str(e)}
