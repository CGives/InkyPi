import threading
import time
import os
import logging
import pytz
from datetime import datetime, timezone

# Absolute imports for plugin_registry, image_utils, model, etc.
from plugins.plugin_registry import get_plugin_instance
from utils.image_utils import compute_image_hash
from model import RefreshInfo, PlaylistManager
from PIL import Image


logger = logging.getLogger(__name__)

class RefreshTask:
    def __init__(self, display_manager, device_config):
        self.display_manager = display_manager
        self.device_config = device_config

        self.running = False
        self.thread = None
        self.refresh_event = Event()
        self.refresh_result = {}
        self.manual_update_request = ()
        self.condition = Condition()

    def start(self):
        self.running = True
        self.thread = Thread(target=self._run, daemon=True)
        self.thread.start()

    def stop(self):
        self.running = False
        with self.condition:
            self.condition.notify()
        self.thread.join()

    def _run(self):
        """
        Main refresh loop executed by the background thread.

        This version adds robust error handling at each step of the loop to ensure:
        - Individual plugin failures do not terminate the loop.
        - Logging is comprehensive and helpful for debugging.
        - Device config updates are safe and recoverable.
        """
        while True:
            try:
                with self.condition:
                    sleep_time = self.device_config.get_config("scheduler_sleep_time")

                    self.condition.wait(timeout=sleep_time)
                    self.refresh_result = {}
                    self.refresh_event.clear()

                    if not self.running:
                        break

                    playlist_manager = self.device_config.get_playlist_manager()
                    latest_refresh = self.device_config.get_refresh_info()
                    current_dt = self._get_current_datetime()

                    refresh_action = None
                    if self.manual_update_request:
                        logger.info("Manual update requested")
                        refresh_action = self.manual_update_request
                        self.manual_update_request = ()
                    else:
                        logger.info(f"Running interval refresh check. | current_time: {current_dt.strftime('%Y-%m-%d %H:%M:%S')}")
                        playlist, plugin_instance = self._determine_next_plugin(playlist_manager, latest_refresh, current_dt)
                        if plugin_instance:
                            refresh_action = PlaylistRefresh(playlist, plugin_instance)

                    if refresh_action:
                        try:
                            plugin_config = self.device_config.get_plugin(refresh_action.get_plugin_id())
                            plugin = get_plugin_instance(plugin_config)

                            if not plugin:
                                logger.error(f"Could not load plugin instance: {refresh_action.get_plugin_id()}")
                                continue

                            try:
                                image = refresh_action.execute(plugin, self.device_config, current_dt)
                            except Exception as e:
                                logger.exception(f"Error executing plugin: {refresh_action.get_plugin_id()}")
                                continue

                            try:
                                image_hash = compute_image_hash(image)
                            except Exception as e:
                                logger.exception("Failed to compute image hash")
                                continue

                            refresh_info = refresh_action.get_refresh_info()
                            refresh_info.update({
                                "refresh_time": current_dt.isoformat(),
                                "image_hash": image_hash
                            })

                            if image_hash != latest_refresh.image_hash:
                                try:
                                    logger.info(f"Updating display. | refresh_info: {refresh_info}")
                                    self.display_manager.display_image(
                                        image,
                                        image_settings=plugin.config.get("image_settings", [])
                                    )
                                except Exception as e:
                                    logger.exception("Failed to update display")
                                    continue
                            else:
                                logger.info(f"Image already displayed, skipping refresh. | refresh_info: {refresh_info}")

                            self.device_config.refresh_info = RefreshInfo(**refresh_info)

                        except Exception as e:
                            logger.exception("Critical error during refresh action block")
                            continue

                    try:
                        self.device_config.write_config()
                    except Exception as e:
                        logger.exception("Failed to write config to disk")

            except Exception as e:
                logger.exception("Unhandled exception in refresh thread")
                self.refresh_result["exception"] = e
            finally:
                self.refresh_event.set()

    def manual_update(self, playlist, plugin_instance):
        with self.condition:
            self.manual_update_request = PlaylistRefresh(playlist, plugin_instance)
            self.condition.notify()

    def wait_for_refresh(self):
        self.refresh_event.wait()
        return self.refresh_result

    def _determine_next_plugin(self, playlist_manager, latest_refresh, current_dt):
        """
        Determine the next plugin to be executed based on the playlist and current time.
        """
        playlist = playlist_manager.get_playlist(latest_refresh.playlist_id)
        plugin_instance = playlist.get_next_plugin(current_dt) if playlist else None
        return playlist, plugin_instance

    def _get_current_datetime(self):
        """
        Return the current system datetime. This method exists to facilitate testing.
        """
        return datetime.now()

