# Diagram Evaluation Intelligence System (DEIS)

Welcome to the **DEIS Engine**, a highly decoupled, asynchronously scalable GPU microservice cluster designed exclusively to evaluate complex, handwritten educational diagrams.

## What is this repository?
Grading diagrams requires fundamentally different architecture than grading text. You cannot feed an image of a Physics circuit to an LLM and expect an exact, deterministic grade. This repository solves that problem using a **Hybrid ML Pipeline** (YOLOv8 + Computational Geometry + Graph Neural Networks).

It operates completely independently from the main Edexia Backend to prevent heavy PyTorch tensors from blocking the main web server.

### The Pipeline Architecture
DEIS is composed of 4 Kafka-driven microservices that process diagrams in an event-driven pipeline:

1. **Gateway Service (`deis-gateway`)**: Exposes a fast HTTP polling endpoint. Ingests images and immediately pushes them onto the Kafka queue (`diagram.raw.uploaded`).
2. **Detection Service (`deis-detection`)**: A PyTorch worker that loads **YOLOv8** weights. It scans the raw image to extract exact bounding box crops for Anatomical Regions, Arrowheads, and Handwritten Labels.
3. **Structural Graph Service (`deis-structural`)**: Uses `OpenCV` and `NumPy` mathematical geometry (Euclidean Distance formulas) to determine which Label is pointing to which Region. It uses `NetworkX` to construct a directed mathematical **Scene Graph** of the drawing.
4. **Scoring Service (`deis-scoring`)**: Uses `networkx.algorithms.isomorphism.DiGraphMatcher` to perform structural overlap comparison. It overlays the student's mathematical Scene Graph against the teacher's "Golden Rubric Graph" to deterministically calculate partial marks. 

## Tech Stack
- **Message Broker**: Apache Kafka + Zookeeper
- **Web Layer**: FastAPI + Uvicorn
- **Computer Vision**: Ultralytics (YOLOv8), OpenCV-Python
- **Graph Mathematics**: NetworkX, NumPy
- **Orchestration**: Docker Compose

## Running the Cluster
```bash
docker-compose -f docker-compose.deis.yml up --build -d
```
You can then post an image to `localhost:8001/api/v1/diagram/evaluate` and watch the payload hop through the neural networks!
