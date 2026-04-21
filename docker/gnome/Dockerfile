FROM python:3.10-slim

LABEL maintainer="kkreiju"
LABEL description="Phase 3: Oil Spill Modeling (PyGNOME) - Stable Build"

# 1. System Dependencies
# These are the heavy C/C++ libraries required for compiling the NOAA stack.
# Added 'cmake' as it is required by scikit-build-core for cell_tree2d and PyGNOME.
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    git \
    cmake \
    libnetcdf-dev \
    libhdf5-dev \
    libproj-dev \
    libgeos-dev \
    libgd-dev \
    libgdal-dev \
    libspatialindex-dev \
    g++ \
    curl \
    && rm -rf /var/lib/apt/lists/*

# 2. Build Tools & Backends
# We pre-install the build ecosystem globally. 
# Added scikit-build-core and ninja which are required for the latest cell_tree2d.
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir --root-user-action ignore \
    poetry==1.7.0 setuptools wheel hatchling scikit-build-core ninja maturin \
    "cython<3.0.0" "numpy>=1.26.0,<2.0.0"

WORKDIR /app

# 3. Handle Standard Dependencies via Poetry
# Ensure your pyproject.toml does NOT contain the git+https NOAA links.
COPY docker/gnome/pyproject.toml pyproject.toml
RUN poetry config virtualenvs.create false && \
    poetry install --no-interaction --no-ansi --no-root

# 4. Install the NOAA Stack with its specific build requirements
# Using --no-build-isolation forces the use of our pre-installed NumPy/Cython.
# We set CMAKE_PREFIX_PATH so scikit-build-core can find Cython's CMake modules (UseCython).
ENV CMAKE_PREFIX_PATH="/usr/local/lib/python3.10/site-packages/Cython/CMake:${CMAKE_PREFIX_PATH}"

# Gridded (Stable pre-NumPy 2)
RUN pip install --no-build-isolation --no-deps --root-user-action ignore git+https://github.com/NOAA-ORR-ERD/gridded.git@v0.7.4

# PyGD (The C++ drawing engine)
RUN pip install --no-build-isolation --no-deps --root-user-action ignore git+https://github.com/NOAA-ORR-ERD/py_gd.git@v2.3.2

# PyNucos (Unit conversion)
RUN pip install --no-build-isolation --no-deps --root-user-action ignore git+https://github.com/NOAA-ORR-ERD/pynucos.git@v3.4.1

# CellTree2D (Using stable PyPI version to avoid broken scikit-build-core logic in main branch)
RUN pip install --no-build-isolation --root-user-action ignore cell_tree2d==0.3.0

# Adios Oil Database (The core oil library)
RUN pip install --no-build-isolation --no-deps --root-user-action ignore "git+https://github.com/NOAA-ORR-ERD/adios_oil_database.git@v1.2.7#subdirectory=adios_db"

# 5. Final Step: PyGNOME
RUN CXXFLAGS="${CXXFLAGS} -Wno-narrowing" \
    pip install --no-build-isolation --no-deps --root-user-action ignore "git+https://github.com/NOAA-ORR-ERD/PyGNOME.git@v1.1.18#egg=gnome&subdirectory=py_gnome"

# 6. Final Project Setup
COPY . .

ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
ENV PIP_DEFAULT_TIMEOUT=100
ENV PIP_DISABLE_PIP_VERSION_CHECK=1

# Ensure the root /app is in the PYTHONPATH so 'python -m src' works correctly
ENV PYTHONPATH="/app:${PYTHONPATH}"

CMD ["python", "-m", "src"]
