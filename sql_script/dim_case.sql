-- Create the new table
DROP TABLE IF EXISTS dim_case;
CREATE TABLE dim_case (
    id SERIAL PRIMARY KEY,
    status_name VARCHAR(255),
    status_detail VARCHAR(255),
    status VARCHAR(255)
);

-- Insert data into the new table
INSERT INTO dim_case (status_name, status_detail, status) VALUES
    ('closecontact', 'dikarantina', 'closecontact_dikarantina'),
    ('closecontact', 'discarded', 'closecontact_discarded'),
    ('closecontact', 'meninggal', 'closecontact_meninggal'),
    ('confirmation', 'meninggal', 'confirmation_meninggal'),
    ('confirmation', 'sembuh', 'confirmation_sembuh'),
    ('probable', 'diisolasi', 'probable_diisolasi'),
    ('probable', 'discarded', 'probable_discarded'),
    ('probable', 'meninggal', 'probable_meninggal'),
    ('suspect', 'diisolasi', 'suspect_diisolasi'),
    ('suspect', 'discarded', 'suspect_discarded'),
    ('suspect', 'meninggal', 'suspect_meninggal');
