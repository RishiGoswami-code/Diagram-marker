import os
import json
import logging
import time
from confluent_kafka import Consumer, Producer

try:
    import numpy as np
    import networkx as nx
    HAS_ML = True
except ImportError:
    HAS_ML = False

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAFKA_BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

consumer = Consumer({
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': 'structural_group',
    'auto.offset.reset': 'earliest'
})
producer = Producer({'bootstrap.servers': KAFKA_BROKER})

def euclidean_distance(p1, p2):
    return np.sqrt((p1[0] - p2[0])**2 + (p1[1] - p2[1])**2)

def build_scene_graph(crops):
    """
    Takes YOLO crops, uses Euclidean distance to map Labels -> Arrows -> Regions,
    and returns a NetworkX Directed Graph serialized as a JSON edge list.
    """
    if not HAS_ML:
        logger.warning("ML libraries missing. Using mock graph.")
        return {"nodes": crops, "edges": [{"source": "label_aorta", "target": "region_1", "relation": "points_to"}]}
        
    labels = [c for c in crops if c.get("cls") == 2]
    arrows = [c for c in crops if c.get("cls") == 1]
    regions = [c for c in crops if c.get("cls") == 0]
    
    G = nx.DiGraph()
    
    # Add nodes
    for l in labels:
        G.add_node(f"label_{l['text']}", type="label", text=l['text'], center=l['center'])
    for idx, r in enumerate(regions):
        G.add_node(f"region_{idx}", type="region", center=r['center'])
        
    # Spatial Algorithm: For every arrow, find the closest label to its tail, and closest region to its head
    # (Since YOLO bounding box center doesn't give direction, we assume nearest label and nearest region to arrow center)
    for a in arrows:
        arr_center = a['center']
        
        # Find closest label
        closest_label = None
        min_ldist = float('inf')
        for l in labels:
            d = euclidean_distance(arr_center, l['center'])
            if d < min_ldist:
                min_ldist = d
                closest_label = f"label_{l['text']}"
                
        # Find closest region
        closest_region = None
        min_rdist = float('inf')
        for idx, r in enumerate(regions):
            d = euclidean_distance(arr_center, r['center'])
            if d < min_rdist:
                min_rdist = d
                closest_region = f"region_{idx}"
                
        # Build directed edge: Label points to Region
        if closest_label and closest_region and min_ldist < 200 and min_rdist < 200:
            G.add_edge(closest_label, closest_region, relation="points_to")
            
    logger.info(f"Built Scene Graph with {G.number_of_nodes()} nodes and {G.number_of_edges()} edges.")
    
    # Serialize Graph
    return nx.node_link_data(G)

def main():
    consumer.subscribe(['diagram.regions.extracted'])
    logger.info("Structural Graph Worker started.")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None: continue
            if msg.error(): continue

            payload = json.loads(msg.value().decode('utf-8'))
            task_id = payload.get("task_id")
            logger.info(f"Building scene graph for task {task_id}")

            # ── Execute Spatial Math ──
            graph_data = build_scene_graph(payload.get("diagram_crops"))
            
            out_msg = {
                "task_id": task_id,
                "scene_graph": graph_data,
                "rubric": payload.get("rubric")
            }
            producer.produce('diagram.graph.built', key=task_id, value=json.dumps(out_msg).encode('utf-8'))
            producer.poll(0)
            
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

if __name__ == "__main__":
    main()
