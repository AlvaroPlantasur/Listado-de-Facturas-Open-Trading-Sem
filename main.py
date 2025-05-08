import os
import psycopg2
from openpyxl import load_workbook
from openpyxl.utils import get_column_letter
from datetime import datetime
import sys

def main():
    # 1. Obtener credenciales y la ruta del archivo base
    db_name = os.environ.get('DB_NAME')
    db_user = os.environ.get('DB_USER')
    db_password = os.environ.get('DB_PASSWORD')
    db_host = os.environ.get('DB_HOST')
    db_port = os.environ.get('DB_PORT')
    file_path = os.environ.get('EXCEL_FILE_PATH')

    db_params = {
        'dbname': db_name,
        'user': db_user,
        'password': db_password,
        'host': db_host,
        'port': db_port,
        'sslmode': 'require'
    }

    # 2. Definir la nueva consulta SQL
    query = """
    SELECT
        rc.name as "Compañía",
        a.internal_number as "Número",
        rp.vat as "CIF",
        p.name AS "Cliente/Proveedor",
        CASE
            WHEN a.type = 'out_invoice' THEN 'Cliente'
            WHEN a.type = 'in_invoice' THEN 'Proveedor'
            WHEN a.type = 'out_refund' THEN 'Rectificativa Cliente'
            WHEN a.type = 'in_refund' THEN 'Rectificativa Proveedor'
            ELSE 'Otro'
        END as "Tipo",
        to_char(a.date_invoice, 'DD') as "Fecha Dia",
        to_char(a.date_invoice, 'MM') as "Fecha Mes",
        to_char(a.date_invoice, 'YYYY') as "Fecha Año",
        to_char(a.date_invoice, 'DD/MM/YYYY') as "Fecha",
        CASE
            WHEN afp.name = 'Recargo de Equivalencia' THEN 'Recargo de Equivalencia'
            WHEN afp.name = 'Régimen Extracomunitario' THEN 'Régimen Extracomunitario'
            WHEN afp.name IN ('REGIMEN INTRACOMUNITARIO', 'Régimen Intracomunitario') THEN 'Régimen Intracomunitario'
            WHEN afp.name = 'REGIMEN NACIONAL' THEN 'Régimen Nacional'
            ELSE afp.name
        END as "Régimen fiscal",
        a.amount_untaxed_0 as "Base 0",
        a.amount_taxed_0 as "Importe 0",
        a.amount_total_0 as "Total 0",
        a.amount_untaxed_4 as "Base 4",
        a.amount_taxed_4 as "Importe 4",
        a.amount_total_4 as "Total 4",
        a.amount_untaxed_8 as "Base 8",
        a.amount_taxed_8 as "Importe 8",
        a.amount_total_8 as "Total 8",
        a.amount_untaxed_10 as "Base 10",
        a.amount_taxed_10 as "Importe 10",
        a.amount_total_10 as "Total 10",
        a.amount_untaxed_18 as "Base 18",
        a.amount_taxed_18 as "Importe 18",
        a.amount_total_18 as "Total 18",
        a.amount_untaxed_21 as "Base 21",
        a.amount_taxed_21 as "Importe 21",
        a.amount_total_21 as "Total 21",
        a.recargo_equivalencia_05 as "Recargo 0,5",
        a.recargo_equivalencia_1 as "Recargo 1",
        a.recargo_equivalencia_14 as "Recargo 1.4",
        a.recargo_equivalencia_4 as "Recargo 4",
        a.recargo_equivalencia_52 as "Recargo 5.2",
        a.amount_total as "Total",
        (CASE
            WHEN a.type = 'out_refund' THEN
                - (
                    COALESCE(a.amount_untaxed_0, 0) +
                    COALESCE(a.amount_untaxed_4, 0) +
                    COALESCE(a.amount_untaxed_8, 0) +
                    COALESCE(a.amount_untaxed_10, 0) +
                    COALESCE(a.amount_untaxed_18, 0) +
                    COALESCE(a.amount_untaxed_21, 0)
                )
            ELSE
                (
                    COALESCE(a.amount_untaxed_0, 0) +
                    COALESCE(a.amount_untaxed_4, 0) +
                    COALESCE(a.amount_untaxed_8, 0) +
                    COALESCE(a.amount_untaxed_10, 0) +
                    COALESCE(a.amount_untaxed_18, 0) +
                    COALESCE(a.amount_untaxed_21, 0)
                )
        END) as "Total BASES",
        CASE
            WHEN a.obsolescencia = TRUE THEN 'SI'
            ELSE 'NO'
        END as "BULK", -- Columna Obsolescencia (Bulk)
        'S-' || rp.id AS "ID Cliente",
        rpa.city AS "Ciudad"
    FROM
        account_invoice as a
    INNER JOIN
        res_partner as p ON p.id = a.partner_id
    INNER JOIN
        account_fiscal_position afp ON afp.id = a.fiscal_position
    INNER JOIN
        res_company rc ON rc.id = a.company_id
    INNER JOIN
        res_partner rp ON rp.id = a.partner_id
    INNER JOIN
        res_partner_address rpa ON rpa.id = a.address_invoice_id
    WHERE
        a.state IN ('open','paid')
        AND a.date_invoice BETWEEN '2025-04-01' AND '2025-04-05'
        AND a.type IN ('out_invoice', 'out_refund') -- Solo facturas y rectificativas de clientes
        AND a.internal_number NOT LIKE 'RA%' -- Omitir RAPPEL
    ORDER BY
        a.internal_number;
    """

    print("Ejecutando consulta para el rango de fechas especificado.")

    # 4. Ejecutar la consulta y obtener los datos
    try:
        with psycopg2.connect(**db_params) as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                resultados = cur.fetchall()
                headers = [desc[0] for desc in cur.description]
    except Exception as e:
        print(f"Error al conectar o ejecutar la consulta: {e}")
        sys.exit(1)

    if not resultados:
        print("No se obtuvieron resultados de la consulta.")
        return
    else:
        print(f"Se obtuvieron {len(resultados)} filas de la consulta.")

    # 5. Abrir el archivo base
    try:
        book = load_workbook(file_path)
        sheet = book.active
    except FileNotFoundError:
        print(f"No se encontró el archivo base '{file_path}'. Se aborta para no perder el formato.")
        return

    # 6. Evitar duplicados (asumiendo que la columna 'Número' es un buen identificador)
    existing_numbers = {row[1] for row in sheet.iter_rows(min_row=2, values_only=True) if row[1] is not None}
    nuevas_filas_anadidas = 0
    for row_data in resultados:
        if row_data[1] not in existing_numbers:
            sheet.append(row_data)
            nuevas_filas_anadidas += 1
            existing_numbers.add(row_data[1])

    if nuevas_filas_anadidas > 0:
        print(f"Se añadieron {nuevas_filas_anadidas} nuevas filas a la hoja.")
    else:
        print("No se añadieron nuevas filas (o ya existían o no había datos nuevos).")

    # 7. Actualizar la referencia de la tabla existente
    if "Lineas2025" in sheet.tables:
        tabla = sheet.tables["Lineas2025"]
        max_row = sheet.max_row

        if sheet.calculate_dimension() == "A1" and not headers:
            max_col = 0
        elif not headers and max_row == 1 and sheet.max_column == 1 and sheet["A1"].value is None:
            max_col = 0
        elif headers:
            max_col = len(headers)
        else:
            max_col = sheet.max_column

        if max_row > 0 and max_col > 0:
            last_col_letter = get_column_letter(max_col)
            new_ref = f"A1:{last_col_letter}{max_row}"
            tabla.ref = new_ref
            print(f"Tabla 'Lineas2025' actualizada a rango: {new_ref}")
        elif "Lineas2025" in sheet.tables :
            print("La tabla 'Lineas2025' existe pero no se pudo determinar un nuevo rango válido.")
    else:
        print("No se encontró la tabla 'Lineas2025'.")

    # 8. Guardar el libro
    try:
        book.save(file_path)
        print(f"Archivo guardado con los datos actualizados en '{file_path}'.")
    except Exception as e:
        print(f"Error al guardar el archivo '{file_path}': {e}")

if __name__ == '__main__':
    main()
