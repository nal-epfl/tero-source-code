from logger import get_logger

from server_changes.sequence_grouper import SequenceGrouper
from server_changes.location_changes_detector import LocationChangeDetection


if __name__ == "__main__":
    logger = get_logger("location_change_analysis-1")

    # Step 1: Group all sequences
    logger.info("Step1: Grouping sequences...")
    # processor = SequenceGrouper(logger)
    # processor.group_sequences()

    # Step 2: Location change analysis
    logger.info("Step3: Location change analysis...")
    location_changes = LocationChangeDetection(logger)
    location_changes.run()