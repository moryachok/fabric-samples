# Fixing "Invalid RoleMemberships" Error During Fabric Semantic Model Restore

When restoring an ABF backup of a semantic model into a Fabric workspace (via the XMLA endpoint), you may encounter:

```
Error -1052311405: There are invalid rolememberships in roles,
please remove invalid rolememberships from roles and try again.
```

**Root cause:** Fabric validates every principal in the model's RLS role memberships against Entra ID during restore. If any referenced security group has been deleted from Entra ID, the entire restore is rejected — even though the model works fine in normal operation.

## What the Notebook Does

[notebook.ipynb](notebook.ipynb) detects and identifies orphaned Entra ID groups in a semantic model's Row-Level Security configuration:

1. **Install dependencies** — upgrades `semantic-link-labs` and `semantic-link`.
2. **Initialize** — connects to the Fabric workspace and resolves the target semantic model.
3. **Inspect role memberships** — reads all RLS roles, table-level filter expressions, and members via the Tabular Object Model (TOM).
4. **Detect orphaned groups** — creates a temporary blank semantic model and attempts to add each member one by one. Any member that fails Fabric's own validation is flagged as orphaned.

After running the notebook, remove the orphaned members from the source model and take a fresh ABF backup before restoring.

## Usage

Upload the notebook to a Fabric workspace and run it there. Update the `workspace` and `dataset` variables in the initialization cell to point to your semantic model.

For instructions on how to import, manage, and run notebooks in Microsoft Fabric, see:
[How to use Microsoft Fabric notebooks](https://learn.microsoft.com/en-us/fabric/data-engineering/how-to-use-notebook)

## Related

- [LinkedIn article — The Hidden Blocker in Power BI to Fabric Migrations](linkedin-article-rolememberships.md)
- [semantic-link-labs documentation](https://semantic-link-labs.readthedocs.io/en/latest/modules.html)
- [semantic-link-labs GitHub](https://github.com/microsoft/semantic-link-labs)