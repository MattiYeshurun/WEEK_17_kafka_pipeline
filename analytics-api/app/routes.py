import json
from fastapi import APIRouter, BackgroundTasks

router = APIRouter()

@router.get("/analytics/top-customers")
def seed_data(background_tasks: BackgroundTasks):
    file_path = "users_with_posts.json"
    try:
        with open(file_path, "r") as f:
            all_users = json.load(f)
        background_tasks.add_task(process_seeding, all_users)
        
        return {
            "status": "started",
            "message": "seeding from file in batches of 10 every 5 seconds"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    

@router.get("/analytics/customers-without-orders")


@router.get("/analytics/zero-credit-active-customers")



