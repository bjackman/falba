import argparse
import hashlib
import logging
import os
import pathlib
import shutil
from typing import Any

import polars as pl

import falba


def compare(db: falba.Db, facts_eq: dict[str, Any], experiment_fact: str, metric: str):
    df = db.flat_df()

    # TODO: This should be done in Pandas or DuckDB or something, but don't
    # wanna bake in a schema just now.

    # Raise an error if any facts were specified that don't exist for any
    # result.
    extant_facts = set()
    for result in db.results.values():
        extant_facts |= result.facts.keys()
    missing_facts = set(facts_eq.keys()) - extant_facts
    if missing_facts:
        raise RuntimeError(
            f"Facts {missing_facts} not in any result in DB. Typo? "
            + f"Available facts: {list(extant_facts)}"
        )

    # Filter results based on facts_eq.
    def include_result(result: falba.Result) -> bool:
        for name, required_val in facts_eq.items():
            if name in result.facts and result.facts[name].value != required_val:
                return False
        return True

    results = [r for r in db.results.values() if include_result(r)]

    # Check all facts are either part of the experiment, or equal for all
    # results.
    for fact in extant_facts:
        if fact == experiment_fact or fact in facts_eq:
            continue
        vals = set()
        for result in results:
            if fact in result.facts:
                vals.add(result.facts[fact].value)
            else:
                vals.add(None)
        if len(vals) != 1:
            raise RuntimeError(
                f"Multiple values encountered for fact {fact}: {vals}\n"
                + "Try constraining with --fact-eq"
            )

    # Lol now I switched to Pandas after all.
    df = db.flat_df().lazy()
    results_df = df.filter(pl.col("result_id").is_in({r.result_id for r in results}))
    if not len(results_df.collect()):
        raise RuntimeError("No results matched fact predicates")

    df = results_df.filter(pl.col("metric") == metric).collect()
    if not len(df):
        avail_metrics = results_df.select(pl.col("metric").unique()).collect().rows()
        raise RuntimeError(
            f"No results for metric {metric!r}.\n"
            + f"Available metrics for seclected results: {avail_metrics}"
        )

    anal = (
        df.lazy()
        .group_by(pl.col(experiment_fact))
        .agg(
            pl.len().alias("samples"),
            pl.col("value").mean().alias("mean"),
            pl.col("value").std().alias("std"),
        )
    )
    print(anal.collect())


def import_result(db: falba.Db, test_name: str, artifact_paths: list[pathlib.Path]):
    """Add a result to the database. Update the db in memory too.

    Files specified directly are added by name to the root of the artifacts
    tree. Directories are copied recursively, preserving the their structure.
    """

    # Helper to walk through the files in a way that reflects the structure of
    # the artifacts directory at the end.
    # Yields tuiples of (current path of file, eventual path of file relative to
    # artifacts/)
    def iter_artifacts():
        for input_path in artifact_paths:
            if input_path.is_dir():
                for dirpath, _, filenames in input_path.walk():
                    for filename in filenames:
                        cur_path = dirpath / filename
                        yield cur_path, cur_path.relative_to(input_path)
            else:
                yield input_path, input_path.name

    # Figure out the result ID by hashing the artifacts.
    hash = hashlib.sha256()
    for path, _ in iter_artifacts():
        # Doesn't seem to be a concise way to update a hash object
        # from a file, you can only get a digest for the whole file
        # at once so just do that and then we'll hash the hashes.
        with open(path, "rb") as f:
            hash.update(hashlib.file_digest(f, "sha256").digest())

    # Copy the artifacts into the database.
    result_dir = db.root_dir / f"{test_name}:{hash.hexdigest()[:12]}"
    # This must fail if the directory already exists.
    os.mkdir(result_dir)
    artifacts_dir = result_dir / "artifacts"
    num_copied = 0
    for cur_path, artifact_relpath in iter_artifacts():
        artifact_path = artifacts_dir / artifact_relpath
        # Since we know artifacts_dir is new, we don't care if this fails. This
        # means if the user provides duplicate inputs, meh.
        os.makedirs(artifact_path.parent, exist_ok=True)
        shutil.copy(cur_path, artifact_path)
        num_copied += 1

    logging.info(f"Imported {num_copied} artifacts to {result_dir}")


def ls_results(db: falba.Db):
    print(db.results_df())


def ls_metrics(db: falba.Db):
    # TODO: This doesn't print the whole DataFrame unless it's very small.
    print(db.flat_df())


def main():
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )

    parser = argparse.ArgumentParser(description="Falba CLI")
    parser.add_argument("--result-db", default="./results", type=pathlib.Path)

    subparsers = parser.add_subparsers(dest="command")
    subparsers.required = True

    def cmd_ab(args: argparse.Namespace):
        compare(
            db,
            {name: val for [name, val] in args.fact_eq},
            args.experiment_fact,
            args.metric,
        )

    compare_parser = subparsers.add_parser("compare", help="Run A/B test")
    compare_parser.add_argument("experiment_fact")
    compare_parser.add_argument("metric")
    compare_parser.add_argument(
        "--fact-eq",
        action="append",
        default=[],
        nargs=2,
        metavar=("fact", "value"),
        help=(
            "Specify a fact and its value (e.g., --fact-eq fact1 val1) "
            + "Comparison will be filtered to only include results matching this equality."
        ),
    )
    compare_parser.set_defaults(func=cmd_ab)

    def cmd_import(args: argparse.Namespace):
        import_result(db, args.test_name, args.file)

    import_parser = subparsers.add_parser(
        "import", help="Import a result to the database"
    )
    import_parser.add_argument("test_name")
    import_parser.add_argument("file", nargs="+", type=pathlib.Path)
    import_parser.set_defaults(func=cmd_import)

    def cmd_ls_results(args: argparse.Namespace):
        ls_results(db)

    ls_parser = subparsers.add_parser("ls-results", help="List results in the database")
    ls_parser.set_defaults(func=cmd_ls_results)

    def cmd_ls_metrics(args: argparse.Namespace):
        ls_metrics(db)

    ls_parser = subparsers.add_parser("ls-metrics", help="List metrics in the database")
    ls_parser.set_defaults(func=cmd_ls_metrics)

    args = parser.parse_args()

    db = falba.read_db(args.result_db)

    args.func(args)


if __name__ == "__main__":
    main()
