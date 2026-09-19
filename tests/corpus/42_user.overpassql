// ion_chi: a frequent, currently-active Minnesota mapper (verified in the
// dataset's own metadata: hundreds of highway edits, most recent well
// after the dataset's anchor timestamp minus a few weeks).
[out:json][timeout:25];
way[highway=primary](user:"ion_chi")({{bbox}});
out count;
