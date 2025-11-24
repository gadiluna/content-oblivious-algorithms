# Content-Oblivious Leader Election Simulator

Python implementations and visual simulators for content-oblivious leader election algorithms on asynchronous rings and 2-edge-connected graphs. The code accompanies the DISC 2025 papers listed in the references.

## Repository Layout
- `VisualRingNetwork.py`: Tk GUI to visualize leader election on rings (oriented and unoriented variants).
- `VisualGraphNetwork.py`: Tk GUI to visualize leader election on general graphs using the 2-edge-connected algorithm.
- `orientedalgorithm.py`: Non-uniform content-oblivious algorithm for oriented rings (logarithmic message pattern).
- `unorientedalgorithm.py`: Content-oblivious algorithm for unoriented rings.
- `leaderelection2connected.py`: Non-uniform leader election for 2-edge-connected graphs.
- `network.py`: Lightweight asynchronous network simulators (ring and general graphs).
- `grid4x4.json`: Example 2-edge-connected topology used by the graph visualizer.

## Requirements
- Python 3.10+
- `networkx` (`pip install networkx`)
- Tkinter (bundled with standard Python on macOS/Linux; ensure it is installed on Windows)

## Quick Start
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install networkx
```

### Visual ring simulator
```bash
python VisualRingNetwork.py
```
- Choose the algorithm (oriented or unoriented) from the dropdown.
- Set IDs directly in the canvas (click a node) or accept the defaults.
- Use Start/Play/Step/Stop to control delivery of pending messages and watch leader election converge.

### Visual general-graph simulator
```bash
python VisualGraphNetwork.py
```
- Browse to a JSON graph (see format below) or use `grid4x4.json`.
- Press Start to spawn asynchronous processes running `LeaderElection2Connected`.
- Use Play/Step to release pending messages; leader/non-leader states color the nodes (green/red).

### Headless experiments
- Oriented ring: `python orientedalgorithm.py`
- Unoriented ring: `python unorientedalgorithm.py`
- 2-edge-connected graphs: `python leaderelection2connected.py --topology grid4x4.json --trials 3 --verbose`

## Graph JSON Format (used by `VisualGraphNetwork.py`)
Nodes include an integer `id` and optional `x`, `y` coordinates for layout. Edges specify each endpoint’s local port and an optional delivery delay:
```json
{
  "nodes": [
    {"id": 1, "x": 50, "y": 50},
    {"id": 2, "x": 130, "y": 50}
  ],
  "edges": [
    {"source": 1, "target": 2, "port_source": 0, "port_target": 1, "delay": 0.05}
  ]
}
```
Ports are local to a node; they only need to be unique per endpoint. Delays are sampled uniformly from `[0, delay]` when enqueuing messages.

## Logging
Verbose modes append network/process events to `log.txt` (or experiment-specific files) to aid debugging and reproducibility.

## References
- Jérémie Chalopin, Yi-Jun Chang, Lyuting Chen, Giuseppe Antonio Di Luna, Haoran Zhou: Content-Oblivious Leader Election in 2-Edge-Connected Networks. DISC 2025: 21:1-21:22.
- Jérémie Chalopin, Yi-Jun Chang, Lyuting Chen, Giuseppe Antonio Di Luna, Haoran Zhou: Brief Announcement: Non-Uniform Content-Oblivious Leader Election on Oriented Asynchronous Rings. DISC 2025: 51:1-51:7.
