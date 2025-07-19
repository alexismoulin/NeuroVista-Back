import logging
from pathlib import Path
from typing import Dict, Tuple, Union, TextIO

import nibabel as nib
import numpy as np
from nibabel.spatialimages import SpatialImage
from nibabel.affines import apply_affine
from skimage.measure import marching_cubes
import trimesh

logger = logging.getLogger(__name__)

Color = Tuple[int, int, int, int]
LUTEntry = Tuple[str, Color]

def load_lut(lut_path: Path) -> Dict[int, LUTEntry]:
    lut: Dict[int, LUTEntry] = {}
    with lut_path.open() as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split()
            if len(parts) < 6:
                logger.warning("Skipping malformed LUT line: %s", line)
                continue
            try:
                label = int(parts[0])
            except ValueError:
                logger.warning("Skipping LUT line with non-integer label: %s", line)
                continue
            # Assume last 4 are RGBA; middle is name (may contain spaces)
            *name_parts, r, g, b, a = parts[1:]
            name = " ".join(name_parts)
            try:
                lut[label] = (name, (int(r), int(g), int(b), int(a)))
            except ValueError:
                logger.warning("Bad RGBA in LUT line: %s", line)
    return lut

def deterministic_color(label: int) -> Color:
    return (
        (label * 37) % 256,
        (label * 67) % 256,
        (label * 97) % 256,
        255,
    )

def mgz_labels_to_gltf(
    mgz_path: Path,
    lut_path: Path,
    out_gltf: Union[str, Path, TextIO],
    min_vertices: int = 0,
    log_every: bool = True
) -> None:
    """
    Convert a labeled FreeSurfer MGZ segmentation volume to a multi-mesh GLTF scene.

    Parameters
    ----------
    mgz_path : Path
        Path to .mgz volume with integer labels.
    lut_path : Path
        Path to LUT mapping label -> name + RGBA.
    out_gltf : str | Path | file-like
        Output destination for GLTF/GLB (trimesh chooses format by extension).
    min_vertices : int
        Skip meshes whose extracted surface has fewer than this many vertices.
    log_every : bool
        If True, log each processed label.
    """
    if not mgz_path.exists():
        raise FileNotFoundError(mgz_path)
    if not lut_path.exists():
        raise FileNotFoundError(lut_path)

    img = nib.load(str(mgz_path))
    if not isinstance(img, SpatialImage):
        raise TypeError(f"Expected SpatialImage, got {type(img)}")

    data = img.get_fdata(dtype=np.int32)
    affine = img.affine
    labels = sorted(l for l in np.unique(data) if l != 0)

    lut = load_lut(lut_path)

    scene = trimesh.Scene()

    for label in labels:
        # Cheap presence test
        if not np.any(data == label):
            continue

        # Optionally crop for performance
        coords = np.argwhere(data == label)
        if coords.size == 0:
            continue
        z0, y0, x0 = coords.min(0)
        z1, y1, x1 = coords.max(0) + 1
        sub: np.ndarray = (data[z0:z1, y0:y1, x0:x1] == label) # type: ignore[assignment]

        # Extract surface
        try:
            verts, faces, normals, _ = marching_cubes(volume=sub.astype(np.uint8), level=0.5)
        except RuntimeError as e:
            logger.warning("Marching cubes failed for label %d: %s", label, e)
            continue

        if len(verts) < min_vertices:
            continue

        # marching_cubes returns (z,y,x); reorder to (x,y,z) in global index space
        verts[:, [0, 1, 2]] = verts[:, [0, 1, 2]]  # (clarity) still (z,y,x)
        verts_xyz = verts[:, [2, 1, 0]]
        # Add crop offsets
        verts_xyz[:, 0] += x0
        verts_xyz[:, 1] += y0
        verts_xyz[:, 2] += z0

        # Apply affine to get world coordinates
        verts_world = apply_affine(aff=affine, pts=verts_xyz)

        name, color_rgba = lut.get(label, (f"Label_{label}", deterministic_color(label)))
        r, g, b, a = color_rgba
        vertex_colors = np.tile([r, g, b, a], (verts_world.shape[0], 1)) / 255.0

        mesh = trimesh.Trimesh(
            vertices=verts_world,
            faces=faces,
            vertex_colors=vertex_colors,
            process=False
        )
        mesh.metadata = {"name": f"{label}_{name}"}
        scene.add_geometry(mesh, node_name=f"{label}_{name}")

        if log_every:
            logger.info("Label %d (%s): %d verts, %d faces", label, name, len(verts_world), len(faces))

    scene.export(out_gltf)
    logger.info("Wrote GLTF: %s", out_gltf)