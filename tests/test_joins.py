import json
from batchbrain import step, pipeline, run, plan
from batchbrain.sources import FolderSource
from batchbrain.models import Materialization, MaterializationEdge
from batchbrain.db import get_session

def test_joins_expand_shape(tmp_path):
    # Setup sources
    left_dir = tmp_path / "left"
    left_dir.mkdir()
    (left_dir / "user_1.json").write_text('{"id": 1, "name": "Alice"}')
    (left_dir / "user_2.json").write_text('{"id": 2, "name": "Bob"}')

    right_dir = tmp_path / "right"
    right_dir.mkdir()
    (right_dir / "order_1.json").write_text('{"user_id": 1, "total": 100}')
    (right_dir / "order_2.json").write_text('{"user_id": 1, "total": 50}')
    (right_dir / "order_3.json").write_text('{"user_id": 3, "total": 200}')

    left_src = FolderSource(str(left_dir))
    right_src = FolderSource(str(right_dir))

    @step(name="users", version="1", source="left")
    def users(path: str) -> dict:
        with open(path) as f:
            return json.load(f)

    @step(name="orders", version="1", source="right")
    def orders(path: str) -> dict:
        with open(path) as f:
            return json.load(f)

    @step(name="join_users_orders", version="1", depends_on=["users", "orders"], shape="expand")
    def join_users_orders(users: dict, orders: dict):
        for u_key, u_val in users.items():
            for o_key, o_val in orders.items():
                if u_val["id"] == o_val["user_id"]:
                    yield f"{u_key}::{o_key}", {"user": u_val, "order": o_val}

    @step(name="process_join", version="1", depends_on=["join_users_orders"])
    def process_join(join_users_orders: dict) -> dict:
        return {"report": f"{join_users_orders['user']['name']} spent {join_users_orders['order']['total']}"}

    p = pipeline(
        name="test_join",
        sources={"left": left_src, "right": right_src},
        steps=[users, orders, join_users_orders, process_join]
    )

    # First run
    summary = run(p)
    assert summary.created_count == 2 + 3 + 2 + 1 + 2 # 2 users, 3 orders, 2 joined lanes, 1 expand manifest, 2 process_join
    assert summary.reused_count == 0

    with get_session() as session:
        # Check lineage of expand
        expand_mats = session.query(Materialization).filter_by(step_name="join_users_orders").all()
        # 1 manifest + 2 lanes = 3
        assert len(expand_mats) == 3
        
        manifest_mat = next(m for m in expand_mats if m.metadata_json and "produced_lanes" in m.metadata_json)
        metadata = json.loads(manifest_mat.metadata_json)
        assert set(metadata["produced_lanes"]) == {"user_1.json::order_1.json", "user_1.json::order_2.json"}
        
        lane_mats = [m for m in expand_mats if m.id != manifest_mat.id]
        for lm in lane_mats:
            edges = session.query(MaterializationEdge).filter_by(child_id=lm.id).all()
            # 5 parents total (2 users + 3 orders) since expand depends on the whole collection
            assert len(edges) == 5

    # Second run (cached)
    summary2 = run(p)
    # Everything should be reused (including the 2 users, 3 orders, 1 manifest, 2 lanes, 2 downstream)
    assert summary2.created_count == 0
    assert summary2.reused_count > 0

    # Test plan
    plan_result = plan(p)
    for item in plan_result.items:
        if item.step_name == "join_users_orders":
            if item.coordinate == "user_1.json::order_1.json":
                assert item.action == "reuse"

    # Add a new order
    (right_dir / "order_4.json").write_text('{"user_id": 2, "total": 300}')
    
    summary3 = run(p)
    # Created: 1 order, 1 new manifest, 3 new lanes (new input_hash), 1 new downstream
    assert summary3.created_count == 6
    
    with get_session() as session:
        # Check the new manifest has 3 lanes
        expand_mats = session.query(Materialization).filter_by(step_name="join_users_orders").all()
        manifest_mats = [m for m in expand_mats if m.metadata_json and "produced_lanes" in m.metadata_json]
        assert len(manifest_mats) == 2 # 1 from first run, 1 from new run
