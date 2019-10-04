import create_acquisitions
import json

poly = {
    "type": "Polygon",
    "coordinates": [
        [
            [-0.8527, 63.343],
            [-0.0324, 61.7489],
            [-355.2919, 62.1786],
            [-355.8487, 63.7841],
            [-0.8527, 63.343]
        ]
    ]
}

print(json.dumps(create_acquisitions.valid_es_geometry(poly)))