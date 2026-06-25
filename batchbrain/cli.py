import argparse
import sys
import json
from batchbrain.registry import list_processors, get_processor
from batchbrain.processor_runner import run_processor

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
    
    args = parser.parse_args()
    args.func(args)

if __name__ == "__main__":
    main()
