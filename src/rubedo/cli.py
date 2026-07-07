import argparse
import json
import sys
from rich.console import Console
from rich.table import Table

from .db import get_session
from .queries import get_recent_runs, get_run_summary, get_run_failures
from .invalidation import invalidate
from .selection import Selection

console = Console()

def cmd_ls(args):
    with get_session() as session:
        runs = get_recent_runs(session, limit=args.limit)
    
    if not runs:
        console.print("No runs found.")
        return
        
    table = Table(show_header=True, header_style="bold magenta")
    table.add_column("ID", style="dim")
    table.add_column("Pipeline")
    table.add_column("Status")
    table.add_column("Created / Reused")
    table.add_column("Failed")
    table.add_column("Started At")
    
    for r in runs:
        pipeline = r.pipeline_id or "(none)"
        stats = f"{r.created_count} / {r.reused_count}"
        table.add_row(
            r.id, 
            pipeline, 
            r.status, 
            stats, 
            str(r.failed_count), 
            r.started_at
        )
    console.print(table)


def cmd_show(args):
    with get_session() as session:
        run = get_run_summary(session, args.run_id)
        if not run:
            console.print(f"[red]Run {args.run_id} not found.[/red]")
            sys.exit(1)
            
        if args.failed:
            failures = get_run_failures(session, args.run_id)
            if args.json:
                print(json.dumps(failures, indent=2))
                return
            
            if not failures:
                console.print("No failures recorded for this run.")
                return
                
            table = Table(title=f"Failures for Run {args.run_id}", show_header=True)
            table.add_column("Step")
            table.add_column("Coordinate")
            table.add_column("Error Type")
            table.add_column("Message")
            
            for f in failures:
                table.add_row(
                    str(f["step_name"]), 
                    str(f["coordinate"]), 
                    str(f["error_type"]), 
                    str(f["error_message"])
                )
            console.print(table)
            return

    if args.json:
        print(run.model_dump_json(indent=2))
        return
        
    console.print(f"[bold]Run ID:[/bold] {run.id}")
    console.print(f"[bold]Pipeline:[/bold] {run.pipeline_id or '(none)'}")
    console.print(f"[bold]Status:[/bold] {run.status}")
    console.print(f"[bold]Started At:[/bold] {run.started_at}")
    console.print(f"[bold]Finished At:[/bold] {run.finished_at or 'N/A'}")
    console.print(f"[bold]Summary:[/bold] Created: {run.created_count}, Reused: {run.reused_count}, Failed: {run.failed_count}, Blocked: {run.blocked_count}, Filtered: {run.filtered_count}")
    
    if run.by_step:
        table = Table(title="Step Outcomes", show_header=True)
        table.add_column("Step")
        table.add_column("Created")
        table.add_column("Reused")
        table.add_column("Failed")
        table.add_column("Blocked")
        table.add_column("Filtered")
        for step_name, counts in run.by_step.items():
            table.add_row(
                step_name,
                str(counts.get("created", 0)),
                str(counts.get("reused", 0)),
                str(counts.get("failed", 0)),
                str(counts.get("blocked", 0)),
                str(counts.get("filtered", 0))
            )
        console.print(table)


def cmd_invalidate(args):
    try:
        selection = Selection.parse(args.selection)
    except Exception as e:
        console.print(f"[red]Error parsing selection:[/red] {e}")
        sys.exit(1)
        
    result = invalidate(selection, args.reason)
    console.print(f"Invalidated [bold green]{result['invalidated_count']}[/bold green] materializations.")
    console.print(f"New Run ID recorded for invalidation: [cyan]{result['run_id']}[/cyan]")


def main():
    parser = argparse.ArgumentParser(description="Rubedo Read-Only Ops CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)
    
    parser_ls = subparsers.add_parser("ls", help="List recent runs")
    parser_ls.add_argument("--limit", type=int, default=50, help="Number of runs to show")
    parser_ls.set_defaults(func=cmd_ls)
    
    parser_show = subparsers.add_parser("show", help="Show details for a specific run")
    parser_show.add_argument("run_id", help="The Run ID to show")
    parser_show.add_argument("--json", action="store_true", help="Output as JSON")
    parser_show.add_argument("--failed", action="store_true", help="Show failure details")
    parser_show.set_defaults(func=cmd_show)
    
    parser_inv = subparsers.add_parser("invalidate", help="Invalidate materializations by selection query")
    parser_inv.add_argument("selection", help="Selection query (e.g., 'pipeline:my-pipe step:extract')")
    parser_inv.add_argument("--reason", required=True, help="Reason for invalidation")
    parser_inv.set_defaults(func=cmd_invalidate)
    
    args = parser.parse_args()
    args.func(args)

if __name__ == "__main__":
    main()
