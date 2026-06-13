import jobs_scrape_pipeline
import jobs_alerts_pipeline
import standardization_pipeline
import dubzl_crs
import crswth_crs
import allsopp_crs
import acres99_crs
import housing_crs
import property_launches_pipeline
import builder_profiles_pipeline
import property_projects_pipeline
import market_events_pipeline


if __name__ == "__main__":
    import uvicorn
    import errno

    try:
        uvicorn.run("plombery:get_app", reload=True, factory=True, reload_dirs="..", port=8080, host="0.0.0.0")
    except OSError as e:
        if e.errno == errno.EADDRINUSE:
            print(f"ERROR: Port 8080 is already in use. Kill the existing process or change the port.")
            print("  Try: lsof -i :8080  or  fuser -k 8080/tcp")
        else:
            raise
