# ML Model Bundle (Dev → Prod)

Databricks Asset Bundle for training and promoting an ML model (e.g. `wine_quality_model`) from dev to prod using Unity Catalog.

## Workspaces and catalogs

- **Dev workspace** (`dbc-3a9c6033-1f4e`): catalog `hilton_hotmail` (source model).
- **Prod workspace** (`dbc-a97d2600-d6e2`): catalog `prod_2` (target model).

## When dev and prod use the same metastore

If both workspaces are linked to the **same Unity Catalog metastore** (so both can see `hilton_hotmail` and `prod_2`):

1. Deploy to dev: `databricks bundle deploy -t dev`
2. In the **dev** workspace, run the job **"[dev] model_promotion_job"**.

The promotion job will copy `hilton_hotmail.default.wine_quality_model` → `prod_2.default.wine_quality_model`.

## When dev and prod use different metastores (your case)

If you see:

- In **dev**: `PERMISSION_DENIED: Catalog 'prod_2' is not accessible`
- In **prod**: `PERMISSION_DENIED: Catalog 'hilton_hotmail' is not accessible`

then dev and prod use **different metastores**. `copy_model_version` cannot copy across metastores. Use one of these approaches:

### Option 1: Single metastore (recommended if possible)

Ask your account admin to **link both workspaces to the same Unity Catalog metastore**. Then both catalogs are visible from one workspace and you can run the promotion job in dev as above.

### Option 2: Delta Sharing

1. In the **dev** workspace, use Delta Sharing to share the model `hilton_hotmail.default.wine_quality_model`.
2. In the **prod** workspace, create a job that receives the shared model and registers it into `prod_2.default.wine_quality_model`.

### Option 3: Copy artifact to shared path, then register in prod

1. In **dev**: copy the model artifact (the run’s artifact directory: `MLmodel` + `artifacts/`) to a location that **prod** can read (e.g. a shared UC volume or cloud storage).
2. Deploy to prod: `databricks bundle deploy -t prod` (with prod profile).
3. In the **prod** workspace, run **"[prod] register_model_from_path"** and set the job parameter **artifact_path** to that path (e.g. `/Volumes/catalog/schema/volume/path_to_model`).

The `register_from_path` job registers the model at that path into `prod_2.default.wine_quality_model`.

## Deploy and run

- **Dev** (default): `databricks bundle deploy -t dev` then run jobs in the dev workspace.
- **Prod**: `databricks bundle deploy -t prod --profile prod` (ensure `~/.databrickscfg` has a `[prod]` profile for the prod workspace) then run jobs in the prod workspace.
