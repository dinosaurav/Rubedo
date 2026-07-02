import argparse
import sys
import json
import os
from batchbrain.registry import list_processors, get_processor
from batchbrain.processor_runner import run_processor
from batchbrain.db import get_session
from batchbrain.models import Materialization, MaterializationEdge, RunCoordinateStatus, Run, RunEvent
from batchbrain.hashing import hash_json, compute_output_address
from batchbrain.scanner import scan_file
from batchbrain.runner import topological_sort

def list_cmd(args):
    procs = list_processors()
    print(f"{'ID':<20} | {'Name':<20} | {'Folder':<20}")
    print("-" * 65)
    for p in procs:
        print(f"{p.id:<20} | {p.name:<20} | {p.folder:<20}")

def show_dag_cmd(args):
    try:
        p = get_processor(args.pipeline_name)
    except ValueError as e:
        print(e)
        sys.exit(1)
        
    print(f"Pipeline: {p.name}\n")
    print("Steps:")
    for s in p.steps:
        deps = f"depends_on=[{', '.join(s.depends_on)}]"
        print(f"  {s.name:<15} version={s.version:<10} {deps}")
    
    has_edges = any(s.depends_on for s in p.steps)
    if has_edges:
        print("\nEdges:")
        for s in p.steps:
            for dep in s.depends_on:
                print(f"  {dep} -> {s.name}")

def show_cmd(args):
    try:
        p = get_processor(args.processor_id)
        print(f"ID: {p.id}")
        print(f"Name: {p.name}")
        print(f"Folder: {p.folder}")
        print(f"Allow Folder Override: {p.allow_folder_override}")
        show_dag_cmd(argparse.Namespace(pipeline_name=args.processor_id))
    except ValueError as e:
        print(e)
        sys.exit(1)

def run_cmd(args):
    input_dict = {}
    if args.inputs:
        try:
            input_dict = json.loads(args.inputs)
        except json.JSONDecodeError:
            print("Error: --inputs must be valid JSON.")
            sys.exit(1)
            
    try:
        summary = run_processor(
            processor_id=args.processor_id,
            inputs=input_dict,
            force=args.force,
            folder=args.folder,
            workers=args.workers
        )
        print("\nExecution Summary:")
        print(f"Run ID: {summary.run_id}")
        print(f"Total processed: {summary.created_count + summary.reused_count + summary.failed_count}")
        print(f"Outputs created: {summary.created_count}")
        print(f"Outputs reused: {summary.reused_count}")
        print(f"Outputs invalidated: {summary.removed_count}")
    except Exception as e:
        print(f"Error running pipeline: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

def show_mat_cmd(args):
    with get_session() as session:
        mat = session.query(Materialization).filter_by(output_address=args.address).first()
        if not mat:
            print(f"Materialization {args.address} not found")
            return
        
        print(f"ID: {mat.id}")
        print(f"Output Address: {mat.output_address}")
        print(f"Pipeline: {mat.processor_name}")
        print(f"Step: {mat.step_name}")
        print(f"Code Version: {mat.code_version}")
        print(f"Input Hash: {mat.input_hash}")
        print(f"Config Hash: {mat.config_hash}")
        print(f"Output Content Hash: {mat.output_content_hash}")
        print(f"Output Path: {mat.output_path}")
        print(f"Created At: {mat.created_at}")
        
        parents = session.query(Materialization).join(MaterializationEdge, Materialization.id == MaterializationEdge.parent_id).filter(MaterializationEdge.child_id == mat.id).all()
        children = session.query(Materialization).join(MaterializationEdge, Materialization.id == MaterializationEdge.child_id).filter(MaterializationEdge.parent_id == mat.id).all()
        
        if parents:
            print("\nParents:")
            for p in parents:
                print(f"  {p.step_name} {p.output_address}")
        
        if children:
            print("\nChildren:")
            for c in children:
                print(f"  {c.step_name} {c.output_address}")

def show_run_cmd(args):
    with get_session() as session:
        run = session.query(Run).filter_by(id=args.run_id).first()
        if not run:
            print(f"Run {args.run_id} not found")
            return
            
        print(f"Run {run.id}")
        print(f"Pipeline: {run.processor_name}")
        print(f"Status: {run.status}")
        
        summary = {}
        if run.summary_json:
            try:
                summary = json.loads(run.summary_json)
            except:
                pass
                
        print("\nSummary:")
        total = summary.get("total", summary)
        print(f"  total:")
        for k in ["created", "reused", "failed", "blocked", "removed"]:
            print(f"    {k}: {total.get(k, 0)}")
            
        if "by_step" in summary:
            print("\n  by step:")
            for sname, sstats in summary["by_step"].items():
                print(f"    {sname}:")
                for k in ["created", "reused", "failed", "blocked", "removed"]:
                    print(f"      {k}: {sstats.get(k, 0)}")
        
        if args.verbose:
            query = session.query(RunCoordinateStatus).filter_by(run_id=run.id)
            if args.status:
                query = query.filter_by(status=args.status)
            if args.step:
                query = query.filter_by(step_name=args.step)
            coords = query.all()
            
            if coords:
                print("\nCoordinates:")
                for c in coords:
                    print(f"  {c.coordinate:<30} {c.step_name:<15} {c.status:<10} {c.output_address or '-'}")

def show_events_cmd(args):
    with get_session() as session:
        events = session.query(RunEvent).filter_by(run_id=args.run_id).order_by(RunEvent.id).all()
        if not events:
            print(f"No events found for run {args.run_id}")
            return
            
        for e in events:
            coord_str = f" {e.coordinate}" if e.coordinate else ""
            step_str = f" step={e.step_name}" if e.step_name else ""
            print(f"{e.timestamp} {e.event_type}{coord_str}{step_str}")

def explain_cmd(args):
    p = get_processor(args.processor_id)
    coord = args.coordinate
    
    abs_path = os.path.abspath(os.path.join(p.folder, coord))
    if not os.path.exists(abs_path):
        print(f"File not found: {abs_path}")
        sys.exit(1)
        
    sf = scan_file(p.folder, coord)
    
    print(f"Pipeline: {p.name}")
    print(f"Coordinate: {coord}\n")
    
    topo_steps = topological_sort(p)
    
    class FakeMat:
        def __init__(self, och, addr):
            self.output_content_hash = och
            self.output_address = addr
            
    parent_mats = {}
    
    for step in topo_steps:
        is_requested = (not args.step) or (step.name == args.step)
        
        input_hash = sf.content_hash
        if step.depends_on:
            parent_hashes = {dep: parent_mats[dep].output_content_hash for dep in sorted(step.depends_on)}
            if len(step.depends_on) == 1:
                input_hash = parent_hashes[step.depends_on[0]]
            else:
                input_hash = hash_json(parent_hashes)
                
        config_hash = step.config_hash
        output_address = compute_output_address(step.name, step.version, input_hash, config_hash)
        
        if is_requested:
            print(f"Step: {step.name}")
            if step.depends_on:
                for dep in step.depends_on:
                    print(f"  parent: {dep} {parent_mats[dep].output_address}")
            print(f"  input_hash: {input_hash}")
            print(f"  version: {step.version}")
            print(f"  config_hash: {config_hash}")
            print(f"  output_address: {output_address}")
            
            with get_session() as session:
                mat = session.query(Materialization).filter_by(output_address=output_address).first()
                print(f"  materialized: {'yes' if mat else 'no'}")
                print()
                
        with get_session() as session:
            mat = session.query(Materialization).filter_by(output_address=output_address).first()
            if mat:
                parent_mats[step.name] = FakeMat(mat.output_content_hash, mat.output_address)
            else:
                parent_mats[step.name] = FakeMat("unknown_not_materialized", "unknown")

def main():
    parser = argparse.ArgumentParser(description="BatchBrain CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)
    
    proc_parser = subparsers.add_parser("processors", help="Manage pipelines/processors")
    proc_subparsers = proc_parser.add_subparsers(dest="proc_command", required=True)
    
    list_parser = proc_subparsers.add_parser("list", help="List available pipelines")
    list_parser.set_defaults(func=list_cmd)
    
    show_parser = proc_subparsers.add_parser("show", help="Show pipeline details")
    show_parser.add_argument("processor_id", help="The ID of the pipeline")
    show_parser.set_defaults(func=show_cmd)
    
    run_parser = subparsers.add_parser("run", help="Run a pipeline")
    run_parser.add_argument("processor_id", help="The ID of the pipeline to run")
    run_parser.add_argument("--inputs", help="JSON string of inputs", default=None)
    run_parser.add_argument("--force", help="Force recomputation (ignore cache)", action="store_true")
    run_parser.add_argument("--folder", help="Override the source folder", default=None)
    run_parser.add_argument("--workers", help="Override the number of workers", type=int, default=None)
    run_parser.set_defaults(func=run_cmd)
    
    explain_parser = subparsers.add_parser("explain", help="Explain addressing formula for a file")
    explain_parser.add_argument("processor_id", help="The ID of the pipeline")
    explain_parser.add_argument("coordinate", help="The file coordinate relative to the folder")
    explain_parser.add_argument("--step", help="Filter by step name", default=None)
    explain_parser.set_defaults(func=explain_cmd)
    
    show_mat_parser = subparsers.add_parser("show-materialization", help="Show materialization by output address")
    show_mat_parser.add_argument("address", help="The output address")
    show_mat_parser.set_defaults(func=show_mat_cmd)
    
    show_run_parser = subparsers.add_parser("show-run", help="Show details of a specific run")
    show_run_parser.add_argument("run_id", help="The ID of the run")
    show_run_parser.add_argument("--verbose", action="store_true", help="Show coordinate details")
    show_run_parser.add_argument("--status", help="Filter coordinates by status", default=None)
    show_run_parser.add_argument("--step", help="Filter by step name", default=None)
    show_run_parser.set_defaults(func=show_run_cmd)
    
    show_events_parser = subparsers.add_parser("show-events", help="Show event log of a specific run")
    show_events_parser.add_argument("run_id", help="The ID of the run")
    show_events_parser.set_defaults(func=show_events_cmd)
    
    show_dag_parser = subparsers.add_parser("show-dag", help="Show the DAG steps and edges of a pipeline")
    show_dag_parser.add_argument("pipeline_name", help="The ID of the pipeline")
    show_dag_parser.set_defaults(func=show_dag_cmd)
    
    args = parser.parse_args()
    args.func(args)

if __name__ == "__main__":
    main()
