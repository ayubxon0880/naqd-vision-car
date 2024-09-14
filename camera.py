from ultralytics import YOLO
import cv2
from datetime import datetime
import time
import os
import logging

log_folder = f"{os.getcwd()}/python_log"

if not os.path.exists(log_folder):
    os.makedirs(log_folder)

if not os.path.exists(f"{os.getcwd()}/data"):
    os.makedirs(f"{os.getcwd()}/data")

logging.basicConfig(filename=f'{log_folder}/{datetime.now().strftime("%Y-%m-%d")}_camera.log', 
                    level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')


def run_main():
    camera_uri = 'rtsp://admin:hM571632@192.168.199.2/Streaming/Channels/102'
    current_path = os.getcwd()
    model_path = f'{current_path}/w.pt'
    output_file = f"{current_path}/data/{datetime.now().date()}.txt"

    model = YOLO(model_path)
    model.to("cuda")

    object_start_times = {}
    object_durations = {}

    def update_durations(object_key, current_time):
        if object_key not in object_start_times:
            object_start_times[object_key] = current_time
        else:
            if object_key not in object_durations:
                object_durations[object_key] = 0
            object_durations[object_key] += (current_time - object_start_times[object_key])
            object_start_times[object_key] = current_time

    results = model.track(
        source=camera_uri,
        device="cuda",
        classes=[0, 2],
        conf=0.1,
        stream=True
    )

    try:
        last_write_time = time.time()

        for result in results:
            current_time = time.time()
            detected_objects = set()

            for box in result.boxes:
                obj_id = int(box.id) if box.id is not None else None
                class_name = int(box.cls)
                object_key = (class_name, obj_id) if obj_id is not None else (class_name, tuple(box.xyxy))
                detected_objects.add(object_key)
                update_durations(object_key, current_time)

            for object_key in list(object_start_times.keys()):
                if object_key not in detected_objects:
                    update_durations(object_key, current_time)
                    del object_start_times[object_key]

            if current_time - last_write_time >= 5:
                with open(output_file, 'a') as f:
                    for object_key, duration in object_durations.items():
                        class_name, obj_id = object_key
                        f.write(f"{class_name},{obj_id if obj_id is not None else 'N/A'},{duration:.2f},{datetime.now()}\n")
                last_write_time = current_time
                object_durations.clear()

    finally:
        with open(output_file, 'a') as f:
            for object_key, duration in object_durations.items():
                class_name, obj_id = object_key
                f.write(f"{class_name},{obj_id if obj_id is not None else 'N/A'},{duration:.2f},{datetime.now()}\n")

        cv2.destroyAllWindows()

while True:
    try:
        run_main()
        time.sleep(120)
    except Exception as exp:
        print(exp)
        logging.error(f"Error: {exp}")
        time.sleep(20)
