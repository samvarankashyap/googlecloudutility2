"""This file is loaded when starting a new application instance."""
import sys
import os.path

from google.appengine.ext import vendor

# add `lib` subdirectory to `sys.path`, so our `main` module can load
# third-party libraries.
vendor.add('lib')
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'lib'))

