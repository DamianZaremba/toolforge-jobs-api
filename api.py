# Copyright (C) 2021 Arturo Borrero Gonzalez <aborrero@wikimedia.org>
# Copyright (C) 2023 Taavi Väänänen <hi@taavi.wtf>
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
#
import logging
import os

from tjf.api.app import create_app

debug = bool(os.environ.get("DEBUG", None))
skip_metrics = bool(os.environ.get("SKIP_METRICS", None))
skip_images = bool(os.environ.get("SKIP_IMAGES", None))

logging.basicConfig(level=logging.INFO if not debug else logging.DEBUG)
app = create_app(init_metrics=not skip_metrics)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    address = os.environ.get("ADDRESS", "127.0.0.1")
    print("Starting app on {address}:{port}")
    app.run(host=address, port=port, debug=debug, use_reloader=False)
