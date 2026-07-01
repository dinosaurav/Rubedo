import argparse
import sys
import json
from batchbrain.registry import list_processors, get_processor
from batchbrain.processor_runner import run_processor
from batchbrain.db import get_session
from batchbrain.models import Materialization, RunCoordinate
from batchbrain.hashing import hash_json, compute_output_address
from batchbrain.scanner import scan_file

def list_cmd(args):
    procs = list_processors()
    print(f"{'ID':<20} | {'Name':<20} | {'Folder':<20} | {'Version':<15}")
    print("-" * 80)
    for p in procs:
        print(f"{p.id:<20} | {p.name:<20} | {p.folder:<20} | {p.code_version:<15}")

def show_cmd(args):
    try:
        p = get_processor(args.processor_id)
        print(f"ID: {p.id}")
        print(f"Name: {p.name}")
        print(f"Folder: {p.folder}")
        print(f"Code Version: {p.code_version}")
        print(f"Step: {p.step}")
        print(f"Workers: {p.workers}")
        print(f"Allow Folder Override: {p.allow_folder_override}")
        
        if p.input_model:
            print("\nInput Schema:")
            schema = p.input_model.model_json_schema()
            print(json.dumps(schema, indent=2))
        else:
            print("\nInput Schema: None")
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
        print(f"Error running processor: {e}")
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
        print(f"Step: {mat.step}")
        print(f"Code Version: {mat.code_version}")
        print(f"Input Hash: {mat.input_hash}")
        print(f"Config Hash: {mat.config_hash}")
        print(f"Output Content Hash: {mat.output_content_hash}")
        print(f"Output Path: {mat.output_path}")
        print(f"Created At: {mat.created_at}")
        print(f"Created By Run: {mat.created_by_run_id}")
        print(f"Invalidated At: {mat.invalidated_at}")
        print(f"Invalidated By Run: {mat.invalidated_by_run_id}")
        print(f"Invalidation Reason: {mat.invalidation_reason}")

def explain_cmd(args):
    import os
    p = get_processor(args.processor_id)
    coord = args.coordinate
    
    abs_path = os.path.abspath(os.path.join(p.folder, coord))
    if not os.path.exists(abs_path):
        print(f"File not found: {abs_path}")
        sys.exit(1)
        
    sf = scan_file(p.folder, coord)
    
    input_hash = sf.content_hash
    # For explain, we assume empty inputs unless specified (a limitation for this MVP tool)
    config_hash = hash_json({}) 
    output_address = compute_output_address(p.step, p.code_version, input_hash, config_hash)
    
    print(f"processor: {p.name}")
    print(f"coordinate: {coord}")
    print(f"input_hash: {input_hash}")
    print(f"code_version: {p.code_version}")
    print(f"config_hash: {config_hash}")
    print(f"output_address: {output_address}")
    
    with get_session() as session:
        mat = session.query(Materialization).filter_by(output_address=output_address).first()
        print(f"materialized: {'yes' if mat else 'no'}")
        
        last_rc = session.query(RunCoordinate).filter_by(coordinate=coord, output_address=output_address).order_by(RunCoordinate.id.desc()).first()
        if last_rc:
            print(f"status: {last_rc.status} from run {last_rc.run_id}")
        else:
            print(f"status: never run")

def main():
    parser = argparse.ArgumentParser(description="BatchBrain CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)
    
    # Processors sub-command grouping
    proc_parser = subparsers.add_parser("processors", help="Manage processors")
    proc_subparsers = proc_parser.add_subparsers(dest="proc_command", required=True)
    
    list_parser = proc_subparsers.add_parser("list", help="List available processors")
    list_parser.set_defaults(func=list_cmd)
    
    show_parser = proc_subparsers.add_parser("show", help="Show processor details")
    show_parser.add_argument("processor_id", help="The ID of the processor")
    show_parser.set_defaults(func=show_cmd)
    
    # Run command
    run_parser = subparsers.add_parser("run", help="Run a processor")
    run_parser.add_argument("processor_id", help="The ID of the processor to run")
    run_parser.add_argument("--inputs", help="JSON string of inputs", default=None)
    run_parser.add_argument("--force", help="Force recomputation (ignore cache)", action="store_true")
    run_parser.add_argument("--folder", help="Override the source folder", default=None)
    run_parser.add_argument("--workers", help="Override the number of workers", type=int, default=None)
    run_parser.set_defaults(func=run_cmd)
    
    explain_parser = subparsers.add_parser("explain", help="Explain addressing formula for a file")
    explain_parser.add_argument("processor_id", help="The ID of the processor")
    explain_parser.add_argument("coordinate", help="The file coordinate relative to the processor's folder")
    explain_parser.set_defaults(func=explain_cmd)
    
    show_mat_parser = subparsers.add_parser("show-materialization", help="Show materialization by output address")
    show_mat_parser.add_argument("address", help="The output address")
    show_mat_parser.set_defaults(func=show_mat_cmd)
    
    args = parser.parse_args()
    args.func(args)

if __name__ == "__main__":
    main()
