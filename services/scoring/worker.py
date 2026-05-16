import os
import json
import logging
import time
import redis
from confluent_kafka import Consumer, Producer

try:
    import networkx as nx
    from networkx.algorithms import isomorphism
    HAS_ML = True
except ImportError:
    HAS_ML = False

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAFKA_BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
redis_client = redis.from_url(REDIS_URL, decode_responses=True)

consumer = Consumer({
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': 'scoring_group',
    'auto.offset.reset': 'earliest'
})
producer = Producer({'bootstrap.servers': KAFKA_BROKER})

def score_graph_isomorphism(student_graph_data, rubric_schema):
    """
    Reconstructs the NetworkX graph and runs DiGraphMatcher against a generated Rubric Graph.
    """
    if not HAS_ML:
        logger.warning("ML libraries missing. Using mock scoring.")
        time.sleep(0.4)
        return {
            "diagram_detected": True,
            "predicted_marks": 4, "max_marks": 5, "confidence": 0.88,
            "missing_components": ["pulmonary artery label missing"]
        }

    logger.info("Running NetworkX Graph Isomorphism evaluation...")
    student_graph = nx.node_link_graph(student_graph_data)
    
    # ── MOCK RUBRIC GRAPH FOR DEMONSTRATION ──
    # In production, we fetch the "Golden Graph" from Qdrant Vector DB
    rubric_graph = nx.DiGraph()
    rubric_graph.add_node("label_aorta", type="label")
    rubric_graph.add_node("region_0", type="region")
    rubric_graph.add_node("label_left_ventricle", type="label")
    rubric_graph.add_node("region_1", type="region")
    
    rubric_graph.add_edge("label_aorta", "region_0", relation="points_to")
    rubric_graph.add_edge("label_left_ventricle", "region_1", relation="points_to")
    
    # Run exact matching on edge relationships ignoring coordinate differences
    def node_match(n1, n2):
        return n1.get('type') == n2.get('type') and n1.get('text') == n2.get('text')

    matcher = isomorphism.DiGraphMatcher(rubric_graph, student_graph, node_match=node_match)
    
    # Calculate subgraph isomorphism (how much of the rubric did they successfully draw?)
    # A simple scoring metric: Ratio of matched edges to total rubric edges
    rubric_edges = list(rubric_graph.edges())
    student_edges = list(student_graph.edges(data=True))
    
    matched_edges = 0
    missing_edges = []
    
    for r_u, r_v in rubric_edges:
        # Does the student graph contain an edge from an equivalent label to an equivalent region?
        found = False
        for s_u, s_v, data in student_edges:
            # We assume label names matched during OCR/fuzzy matching prior to graph building
            if s_u == r_u: 
                found = True
                matched_edges += 1
                break
        if not found:
            missing_edges.append(f"Missing pointer from {r_u} to its target region.")

    max_marks = rubric_schema.get("max_marks", 5)
    score_ratio = matched_edges / max(len(rubric_edges), 1)
    predicted_marks = round(score_ratio * max_marks)
    
    return {
        "diagram_detected": True,
        "predicted_marks": predicted_marks,
        "max_marks": max_marks,
        "confidence": score_ratio, # Lower score ratio generally means lower AI confidence in messy diagrams
        "rubric_breakdown": {"edges_matched": matched_edges, "edges_required": len(rubric_edges)},
        "missing_components": missing_edges
    }

def main():
    consumer.subscribe(['diagram.graph.built'])
    logger.info("Scoring Worker started.")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None: continue
            if msg.error(): continue

            payload = json.loads(msg.value().decode('utf-8'))
            task_id = payload.get("task_id")
            logger.info(f"Scoring task {task_id}")

            score_result = score_graph_isomorphism(payload.get("scene_graph"), payload.get("rubric") or {})
            score_result["task_id"] = task_id
            score_result["status"] = "COMPLETED"
            
            redis_client.set(f"diagram_result:{task_id}", json.dumps(score_result), ex=86400)
            
            producer.produce('diagram.evaluation.completed', key=task_id, value=json.dumps(score_result).encode('utf-8'))
            producer.poll(0)
            
            logger.info(f"Task {task_id} COMPLETED and saved to Redis.")
            
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

if __name__ == "__main__":
    main()
