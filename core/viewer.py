import logging
from pathlib import Path
from typing import Dict, Tuple, Union, TextIO, Optional, List

import nibabel as nib
import numpy as np
from nibabel.spatialimages import SpatialImage
from nibabel.affines import apply_affine
from skimage.measure import marching_cubes
import trimesh

logger = logging.getLogger(__name__)

Color = Tuple[int, int, int, int]
LUTEntry = Tuple[str, Color]
LUT_PATH = Path.cwd() / "FreeSurferColorLUT.txt"


def load_lut() -> Dict[int, LUTEntry]:
    """
    Parse a FreeSurfer-style color lookup table from ``LUT_PATH``.

    Each non-comment, non-empty line is expected to contain::

        <label:int> <name possibly with spaces> <R:int> <G:int> <B:int> <A:int>

    Lines that cannot be parsed are skipped with a warning.

    Returns
    -------
    Dict[int, LUTEntry]
        Mapping from integer label ID to ``(name, (R, G, B, A))`` with
        8-bit channels in the range 0–255.

    Raises
    ------
    FileNotFoundError
        If ``LUT_PATH`` does not exist.
    """
    lut: Dict[int, LUTEntry] = {}
    with LUT_PATH.open() as f:
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
    """
    Produce a stable fallback RGBA color for a label ID.

    The mapping uses simple modular arithmetic to yield reproducible,
    visually distinct colors without consulting the LUT.

    Parameters
    ----------
    label : int
        Integer label identifier.

    Returns
    -------
    Color
        ``(R, G, B, A)`` tuple with 8-bit channels; alpha is always 255.
    """
    return (
        (label * 37) % 256,
        (label * 67) % 256,
        (label * 97) % 256,
        255,
    )

def mgz_labels_to_gltf(
    mgz_path: Path,
    out_gltf: Union[str, Path, TextIO],
    min_vertices: int = 0,
    log_every: bool = True
) -> None:
    """
        Convert a labeled FreeSurfer MGZ segmentation volume into a multi-mesh GLTF scene.

        One mesh is produced per non-zero integer label found in the volume. Vertex
        positions are exported in scanner RAS (``img.affine``) world space. Per-vertex
        RGBA colors are taken from ``FreeSurferColorLUT.txt`` (``LUT_PATH``) when
        available, falling back to :func:`deterministic_color` otherwise.

        Parameters
        ----------
        mgz_path : Path
            Path to a labeled segmentation volume (``.mgz``).
        out_gltf : str | Path | TextIO
            Output target for the GLTF/GLB. If a path or string is given, parent
            directories are created as needed and the format is inferred from the
            extension (``.gltf``/``.glb``). A writable file-like object is also accepted.
        min_vertices : int, optional
            Skip labels whose extracted surface has fewer than this many vertices.
            Default is 0 (export all).
        log_every : bool, optional
            If ``True``, emit an ``INFO`` log line per exported label. Default ``True``.

        Returns
        -------
        None

        Raises
        ------
        FileNotFoundError
            If ``mgz_path`` or ``LUT_PATH`` does not exist.
        TypeError
            If the loaded image is not a :class:`~nibabel.spatialimages.SpatialImage`.

        Notes
        -----
        * **Surface extraction**: Surfaces are obtained with
          :func:`skimage.measure.marching_cubes` at level 0.5. Failures are logged
          and the corresponding label is skipped.
        * **Cropping**: Each label is cropped to its tight voxel bounding box to
          accelerate meshing. Bounding boxes thinner than 2 voxels along any
          dimension are expanded by one voxel on each side (clamped to image
          bounds) so that marching cubes receives an array at least ``2×2×2``. If
          a dimension is still ``< 2`` after expansion, that label is skipped.
        * **Coordinate conventions**: ``marching_cubes`` returns vertices in (z, y, x).
          These are reordered to (x, y, z), shifted by the crop origin, and mapped
          to world coordinates with ``img.affine`` (scanner RAS).
        """
    mgz_path = Path(mgz_path)
    if not mgz_path.exists():
        raise FileNotFoundError(mgz_path)
    if not LUT_PATH.exists():
        raise FileNotFoundError(LUT_PATH)

    img = nib.load(str(mgz_path))
    if not isinstance(img, SpatialImage):
        raise TypeError(f"Expected SpatialImage, got {type(img)}")

    data = img.get_fdata().astype(np.int32)
    affine = img.affine
    labels = sorted(l for l in np.unique(data) if l != 0)

    lut = load_lut()
    scene = trimesh.Scene()

    # Normalize out_gltf target & ensure directory if path-like
    if isinstance(out_gltf, (str, Path)):
        out_gltf = Path(out_gltf)
        out_gltf.parent.mkdir(parents=True, exist_ok=True)
        export_target: Union[str, Path, TextIO] = str(out_gltf)
    else:
        export_target = out_gltf  # file-like

    for label in labels:
        coords = np.argwhere(data == label)
        if coords.size == 0:
            continue

        # initial bbox
        z0, y0, x0 = coords.min(0)
        z1, y1, x1 = coords.max(0) + 1

        # expand thin dims by 1 voxel each side (clamped)
        def expand(lo, hi, dim):
            if hi - lo >= 2:
                return lo, hi
            lo = max(lo - 1, 0)
            hi = min(hi + 1, dim)
            return lo, hi

        z0e, z1e = expand(z0, z1, data.shape[0])
        y0e, y1e = expand(y0, y1, data.shape[1])
        x0e, x1e = expand(x0, x1, data.shape[2])

        sub = (data[z0e:z1e, y0e:y1e, x0e:x1e] == label).astype(np.uint8)

        # final guard: skip if still too thin (e.g., near volume edge)
        if any(d < 2 for d in sub.shape):
            logger.info(
                "Skipping label %d: cropped volume %s too thin after expansion.",
                label, sub.shape
            )
            continue

        # Extract surface
        try:
            verts, faces, normals, _ = marching_cubes(volume=sub, level=0.5)
        except RuntimeError as e:
            logger.warning("Marching cubes failed for label %d: %s", label, e)
            continue

        if len(verts) < min_vertices:
            continue

        # marching_cubes returns (z,y,x); reorder to (x,y,z) & shift
        verts_xyz = verts[:, [2, 1, 0]]
        verts_xyz[:, 0] += x0e
        verts_xyz[:, 1] += y0e
        verts_xyz[:, 2] += z0e

        # Apply affine to world coordinates
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
            logger.info(
                "Label %d (%s): %d verts, %d faces (crop %s).",
                label, name, len(verts_world), len(faces), sub.shape
            )

    scene.export(export_target)
    logger.info("Wrote GLTF: %s", export_target)


def fs_affine(img: SpatialImage) -> np.ndarray:
    """
    Return the preferred FreeSurfer voxel→world transform.

    Attempts to read the FreeSurfer *tkregister RAS* matrix (what Freeview shows)
    from the header via ``get_vox2ras_tkr()``. If unavailable or an error occurs,
    falls back to the image's ``affine`` (scanner RAS).

    Parameters
    ----------
    img : SpatialImage
        Nibabel spatial image.

    Returns
    -------
    numpy.ndarray
        ``4×4`` affine mapping voxel indices ``(i, j, k, 1)`` → RAS coordinates
        ``(x, y, z, 1)``.
    """
    hdr = getattr(img, "header", None)
    if hdr is not None and hasattr(hdr, "get_vox2ras_tkr"):
        try:
            return hdr.get_vox2ras_tkr()
        except Exception:
            pass
    return img.affine


def combine_mgzs_to_gltf(
    lh_mgz: Path,
    rh_mgz: Path,
    out_gltf: Path,
    min_vertices: int = 0,
    gap_mm: float = 30.0,
) -> None:
    """
    Merge left- and right-hemisphere MGZ segmentations into one GLTF and
    translate them apart along the RAS-X axis so they do not overlap.

    Parameters
    ----------
    lh_mgz, rh_mgz : Path
        Left/right hemisphere MGZ segmentations.
    out_gltf : Path
        Output ``.glb``/``.gltf`` path (format inferred from extension).
    min_vertices : int
        Skip labels whose surface has fewer than this many vertices. Default 0.
    gap_mm : float
        Distance in millimeters added between hemispheres (center-to-center).
        LH is shifted by ``-gap_mm/2`` and RH by ``+gap_mm/2`` along RAS-X.

    Returns
    -------
    None

    Raises
    ------
    FileNotFoundError
        If any input MGZ or the LUT file is missing.
    TypeError
        If a loaded image is not a :class:`~nibabel.spatialimages.SpatialImage`.

    Notes
    -----
    * Vertex positions are computed in FreeSurfer *tkregister RAS* space when
      available (via :func:`_fs_affine`), matching Freeview, then offset along
      RAS-X by ``±gap_mm/2``.
    * Surfaces are extracted per label using :func:`skimage.measure.marching_cubes`
      at level 0.5, operating on a cropped bounding box in voxel index order
      ``(i, j, k)``. ``marching_cubes`` returns vertices in the same axis order.
    * Colors come from the LUT when present, else :func:`deterministic_color`.
    """
    lut = load_lut()
    scene = trimesh.Scene()

    # Offsets in RAS (mm). X is left-right: LH negative, RH positive.
    offsets = {
        "lh": np.array([-gap_mm / 2.0, 0.0, 0.0], dtype=float),
        "rh": np.array([+gap_mm / 2.0, 0.0, 0.0], dtype=float),
    }

    for side, mgz_path in (("lh", lh_mgz), ("rh", rh_mgz)):
        img = nib.load(str(mgz_path))
        if not isinstance(img, SpatialImage):
            raise TypeError(f"{mgz_path} is not a spatial image")

        data = img.get_fdata().astype(np.int32)
        affine = fs_affine(img)

        labels = sorted(int(l) for l in np.unique(data) if l != 0)
        for label in labels:
            mask = (data == label)
            if not mask.any():
                continue

            # Crop bbox in voxel index order (i, j, k) == (X, Y, Z)
            coords = np.argwhere(mask)  # -> (i, j, k)
            i0, j0, k0 = coords.min(0)
            i1, j1, k1 = coords.max(0) + 1
            sub = mask[i0:i1, j0:j1, k0:k1].astype(np.uint8)

            try:
                # marching_cubes returns verts in SAME axis order as the array: (i, j, k)
                verts, faces, normals, _ = marching_cubes(sub, level=0.5)
            except RuntimeError:
                logger.warning("MC failed on %s label %d", side, label)
                continue
            if len(verts) < min_vertices:
                continue

            # Shift verts back into full-volume voxel index space (i, j, k)
            verts_vox = verts + np.array([i0, j0, k0], dtype=verts.dtype)

            # Map voxel indices -> world (tkregister RAS if available)
            verts_world = apply_affine(affine, verts_vox)

            # Push hemispheres apart along RAS-X
            verts_world = verts_world + offsets[side]

            # Color & name
            name, rgba = lut.get(label, (f"Label_{label}", deterministic_color(label)))
            r, g, b, a = rgba
            vertex_colors = np.tile([r, g, b, a], (len(verts_world), 1)) / 255.0

            mesh = trimesh.Trimesh(
                vertices=verts_world,
                faces=faces,
                vertex_colors=vertex_colors,
                process=False
            )
            prefixed = f"{side} - {name}"
            mesh.metadata = {"name": prefixed}
            scene.add_geometry(mesh, node_name=prefixed)

            logger.info(
                "Added %s label %d (%s): %d verts (mean RAS-X after offset = %.2f)",
                side, label, name, len(verts_world), float(verts_world[:, 0].mean())
            )

    scene.export(str(out_gltf))
    logger.info("Wrote combined GLTF: %s", out_gltf)


def extract_labels_to_gltf(
    mgz_path: Path,
    out_gltf: Union[str, Path, TextIO],
    include_labels: Optional[List[int]] = None,
    include_names:  Optional[List[str]] = None,
    min_vertices:   int   = 0
) -> None:
    """
    Export only a subset of labels from an MGZ to one GLTF.

    Parameters
    ----------
    mgz_path : Path
        ``.mgz`` segmentation volume.
    out_gltf : str | Path | file-like
        Where to write the GLTF/GLB. Parent directories are created if needed
        when a path is provided; the format is inferred from the extension.
    include_labels : list of int, optional
        Whitelist of integer label IDs to export. If ``None``, include all
        labels (subject to ``include_names``).
    include_names : list of str, optional
        Whitelist of structure names (from the LUT). If provided, this takes
        precedence over ``include_labels``. Names not found in the LUT are
        silently ignored.
    min_vertices : int, optional
        Skip labels whose surface has fewer than this many vertices. Default 0.

    Returns
    -------
    None

    Raises
    ------
    FileNotFoundError
        If the LUT file is missing.
    TypeError
        If the loaded image is not a :class:`~nibabel.spatialimages.SpatialImage`.

    Notes
    -----
    * Triangles are extracted per label with :func:`skimage.measure.marching_cubes`
      at level 0.5 on the label's cropped bounding box.
    * Vertex coordinates are exported in scanner RAS (``img.affine``) space.
    * Colors come from the LUT when present, else :func:`deterministic_color`.
    """
    # Load image
    img = nib.load(str(mgz_path))
    if not isinstance(img, SpatialImage):
        raise TypeError(f"{mgz_path} is not a spatial image")
    data = img.get_fdata().astype(np.int32)
    affine = img.affine

    # Load LUT once
    lut = load_lut()  # from earlier helper
    name2label = { name: lab for lab, (name, _) in lut.items() }

    # Decide which labels to export
    all_labels = set(np.unique(data).astype(int)) - {0}
    if include_names:
        wanted = { name2label[n] for n in include_names if n in name2label }
    elif include_labels:
        wanted = set(include_labels) & all_labels
    else:
        wanted = all_labels

    scene = trimesh.Scene()

    for lab in sorted(wanted):
        mask = (data == lab)
        if not mask.any():
            logger.warning("Label %d requested but not present", lab)
            continue

        # Crop to bounding box
        coords = np.argwhere(mask)
        z0, y0, x0 = coords.min(0)
        z1, y1, x1 = coords.max(0) + 1
        sub = mask[z0:z1, y0:y1, x0:x1].astype(np.uint8)

        # Extract surface
        try:
            verts, faces, normals, _ = marching_cubes(sub, level=0.5)
        except RuntimeError as e:
            logger.warning("Marching cubes failed for label %d: %s", lab, e)
            continue
        if len(verts) < min_vertices:
            logger.info("Skipping %d verts (< %d) for label %d", len(verts), min_vertices, lab)
            continue

        # Reorder (z,y,x)->(x,y,z), shift by crop origin, apply affine
        verts_xyz = verts[:, [2,1,0]] + np.array([x0, y0, z0])
        verts_world = nib.affines.apply_affine(affine, verts_xyz)

        # Color & name
        name, rgba = lut.get(lab, (f"Label_{lab}", deterministic_color(lab)))
        r, g, b, a = rgba
        vertex_colors = np.tile([r, g, b, a], (len(verts_world), 1)) / 255.0

        mesh = trimesh.Trimesh(
            vertices=verts_world,
            faces=faces,
            vertex_colors=vertex_colors,
            process=False
        )
        mesh_name = f"{lab}_{name}"
        mesh.metadata = {"name": mesh_name}
        scene.add_geometry(mesh, node_name=mesh_name)

        logger.info("Added label %d (%s) with %d verts", lab, name, len(verts_world))

    # Export single GLTF
    scene.export(str(out_gltf))
    logger.info("Wrote GLTF: %s", out_gltf)


def create_gltf_models(freesurfer_path: Path, viewer_path: Path, folder: str) -> None:
    """
        Build a set of GLTF/GLB models for a FreeSurfer subject.

        Given the root ``freesurfer_path`` containing ``<folder>/mri/`` and an
        output ``viewer_path``, this orchestrates the standard exports:
        - ``mri/aseg.mgz`` → ``aseg.glb``
        - ``mri/brainstemSsLabels.mgz`` → ``brainstemSsLabels.glb``
        - ``mri/ThalamicNuclei.mgz`` → ``ThalamicNuclei.glb``
        - ``mri/hypothalamic_subunits_seg.v1.mgz`` → ``hypothalamic_subunits_seg.v1.glb``
        - ``mri/lh.hippoAmygLabels.mgz`` + ``mri/rh.hippoAmygLabels.mgz`` combined and separated
          along RAS-X → ``hippoAmygLabels.glb``
        - ``mri/wmparc.mgz`` (labels 2000–2035 & 3000–3035) → ``wmparc.glb``
        - ``mri/aparc.DKTatlas+aseg.mgz`` (labels 1002–1035 & 2002–2035) → ``aparc.DKTatlas+aseg.glb``

        Parameters
        ----------
        freesurfer_path : Path
            Root path containing subject folders (e.g., output of ``recon-all``).
        viewer_path : Path
            Destination root. Outputs are written to ``<viewer_path>/<folder>/``.
        folder : str
            Subject folder name (e.g., ``"sub-001"``).

        Returns
        -------
        None

        Notes
        -----
        This is a thin wrapper that delegates work to
        :func:`mgz_labels_to_gltf`, :func:`combine_mgzs_to_gltf`,
        and :func:`extract_labels_to_gltf`. Exceptions raised by those
        functions (e.g., missing files, bad images) will propagate.
        """
    mgz_labels_to_gltf(
        mgz_path=freesurfer_path / folder / "mri" / "aseg.mgz",
        out_gltf=viewer_path / folder / "aseg.glb"
    )
    mgz_labels_to_gltf(
        mgz_path=freesurfer_path / folder / "mri" / "brainstemSsLabels.mgz",
        out_gltf=viewer_path / folder / "brainstemSsLabels.glb"
    )
    mgz_labels_to_gltf(
        mgz_path=freesurfer_path / folder / "mri" / "ThalamicNuclei.mgz",
        out_gltf=viewer_path / folder / "ThalamicNuclei.glb"
    )
    mgz_labels_to_gltf(
        mgz_path=freesurfer_path / folder / "mri" / "hypothalamic_subunits_seg.v1.mgz",
        out_gltf=viewer_path / folder / "hypothalamic_subunits_seg.v1.glb"
    )
    combine_mgzs_to_gltf(
        lh_mgz=freesurfer_path / folder / "mri" / "lh.hippoAmygLabels.mgz",
        rh_mgz=freesurfer_path / folder / "mri" / "rh.hippoAmygLabels.mgz",
        out_gltf=viewer_path / folder / "hippoAmygLabels.glb"
    )
    wm_labels = list(range(2000, 2036)) + list(range(3000, 3036))
    extract_labels_to_gltf(
        mgz_path=freesurfer_path / folder / "mri" / "wmparc.mgz",
        out_gltf=viewer_path / folder / "wmparc.glb",
        include_labels=wm_labels
    )
    ctx_labels = list(range(1002, 1036)) + list(range(2002, 2036))
    extract_labels_to_gltf(
        mgz_path=freesurfer_path / folder / "mri" / "aparc.DKTatlas+aseg.mgz",
        out_gltf=viewer_path / folder / "aparc.DKTatlas+aseg.glb",
        include_labels=ctx_labels
    )
    logger.info(f"GLTF extracted for: {folder}")
