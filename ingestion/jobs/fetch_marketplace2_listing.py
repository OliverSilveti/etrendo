"""Compatibility wrapper that forwards to the dedicated marketplace2 package."""
import argparse

from ingestion.marketplace2 import fetch_marketplace2_listing as impl


def main(argv=None):
    """
    Keep the legacy entrypoint signature while forwarding to the new package.
    Accepts either an argparse.Namespace or argv list for flexibility.
    """
    if isinstance(argv, argparse.Namespace):
        return impl.main(argv)
    return impl.run(argv)


if __name__ == "__main__":
    impl.run()
