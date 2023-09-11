from utils.logger import get_logger

from location_changes.online.group_results_online import ResultsGrouper
from location_changes.online.cluster_detection_online import ClusterDetection
from location_changes.online.location_changes_detection_online import LocationChangeDetection


if __name__ == "__main__":
    logger = get_logger("location_change_analysis_online")

    # Step 1: Group all sequences
    logger.info("Step1: Grouping sequences...")
    processor = ResultsGrouper(logger)
    processor.group_sequences()

    # Step 2: Compute location-based clusters
    logger.info("Step2: Computing location-based clusters...")
    cluster_detector = ClusterDetection(logger)
    cluster_detector.compute_clusters()
    
    # Step 3: Location change analysis
    logger.info("Step3: Location change analysis...")
    location_changes = LocationChangeDetection(logger)
    location_changes.run()