-- A field's profile says whether the field is a floating-point number,
-- which has no shape (decision 0090): the screen names it instead of an
-- empty shape. Earlier cuts keep NULL, unknown. Only adds.
ALTER TABLE quality.field_quality ADD COLUMN IF NOT EXISTS floating boolean;
