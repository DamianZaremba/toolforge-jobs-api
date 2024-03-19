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

from tjf.api.app import create_app

logging.basicConfig(level=logging.INFO)
app = create_app()

if __name__ == "__main__":
    address = "0.0.0.0"
    port = 8080
    print("Starting app on {address}:{port}")
    app.run(host=address, port=port, debug=False, use_reloader=False)
