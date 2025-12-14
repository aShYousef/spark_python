import time
from .spark_jobs import (
    create_spark,
    descriptive_stats,
    linear_regression_job,
    logistic_regression_job,
    kmeans_job,
    fpgrowth_job
)

TASKS = {
    "descriptive_stats": descriptive_stats,
    "linear_regression": linear_regression_job,
    "logistic_regression": logistic_regression_job,
    "kmeans": kmeans_job,
    "fpgrowth": fpgrowth_job
}

def run_jobs(file_path, tasks, workers_list):
    results = []

    for workers in workers_list:
        spark = create_spark(workers)

        for task in tasks:
            start = time.time()
            metrics = TASKS[task](spark, file_path)
            end = time.time()

            results.append({
                "task": task,
                "workers": workers,
                "execution_time": round(end - start, 3),
                "metrics": metrics
            })

        spark.stop()

    return results

