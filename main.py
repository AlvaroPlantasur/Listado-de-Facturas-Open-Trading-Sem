import os
import psycopg2
from openpyxl import load_workbook
from openpyxl.utils import get_column_letter
from datetime import datetime
from dateutil.relativedelta import relativedelta
import sys
import copy

def obtener_credenciales():
    return {
        'dbname': os.environ.get('DB_NAME', 'semillas'),
        'user': os.environ.get('DB_USER', 'openerp'),
        'password': os.environ.get('DB_PASSWORD', ''),
        'host': os.environ.get('DB_HOST', '2.136.142.253'),
        'port': os.environ.get('DB_PORT', '5432')
    }, os.environ.get('EXCEL_FILE_PATH', 'Listado de Facturas Open Trading-Sem 2025.xlsx')

def obtener_fechas():
    end_date = datetime.now()
    start_date = (end_date - relativedelta(months=2)).replace(day=1)
    return start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')

def ejecutar_consulta(conn, query, params):
    with conn.cursor() as cur:
        cur.execute(query, params)
        return cur.fetchall(), [desc[0] for desc in cur.description]

def cargar_archivo_excel(file_path):
    try:
        libro = load_workbook(file_path)
        return libro, libro.active
    except FileNotFoundError:
        print(f"No se encontró el archivo '{file_path}'. Se aborta para no perder el formato.")
        sys.exit(1)

def copiar_formato_fila(source_row, target_row):
    for col in range(1, len(source_row) + 1):
        source_cell = source_row[col - 1]
        target_cell = target_row[col - 1]
        target_cell.font = copy.copy(source_cell.font)
        target_cell.fill = copy.copy(source_cell.fill)
        target_cell.border = copy.copy(source_cell.border)
        target_cell.alignment = copy.copy(source_cell.alignment)

def insertar_sin_duplicados(sheet, datos, columna_clave_idx):
    existentes = {row[columna_clave_idx] for row in sheet.iter_rows(min_row=2, values_only=True)}
    nuevas_filas = 0

    for fila in datos:
        if fila[columna_clave_idx] not in existentes:
            sheet.append(fila)
            copiar_formato_fila(
                [sheet.cell(row=sheet.max_row - 1, column=col) for col in range(1, sheet.max_column + 1)],
                [sheet.cell(row=sheet.max_row, column=col) for col in range(1, sheet.max_column + 1)]
            )
            nuevas_filas += 1
    return nuevas_filas

def actualizar_tabla_excel(sheet, tabla_nombre="Portes"):
    if tabla_nombre in sheet.tables:
        tabla = sheet.tables[tabla_nombre]
        max_row = sheet.max_row
        max_col = sheet.max_column
        ref = f"A1:{get_column_letter(max_col)}{max_row}"
        tabla.ref = ref
        print(f"Tabla '{tabla_nombre}' actualizada a rango: {ref}")
    else:
        print(f"No se encontró la tabla '{tabla_nombre}'.")

def main():
    db_params, file_path = obtener_credenciales()
    start_date, end_date = obtener_fechas()

    query = """SELECT 
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
 
  
	(CASE 							    -- Total Bases (suma de todas las bases) con signo negativo en 'Rectificativa Cliente'
        WHEN a.type = 'out_refund' THEN -- Si es rectificativa cliente
            - (
                COALESCE(a.amount_untaxed_0, 0) +
                COALESCE(a.amount_untaxed_4, 0) +
                COALESCE(a.amount_untaxed_8, 0) +
                COALESCE(a.amount_untaxed_10, 0) +
                COALESCE(a.amount_untaxed_18, 0) +
                COALESCE(a.amount_untaxed_21, 0)
            )
        ELSE -- Para otros tipos de factura, el valor permanece positivo
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
    END as "BULK",  -- Columna Obsolescencia (Bulk)
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
    a.internal_number;"""

    # Conectar y ejecutar consulta
    try:
        with psycopg2.connect(**db_params) as conn:
            resultados, headers = ejecutar_consulta(conn, query, (start_date, end_date))
    except Exception as e:
        print(f"Error al conectar o ejecutar la consulta: {e}")
        sys.exit(1)

    if not resultados:
        print("No se obtuvieron resultados de la consulta.")
        return
    else:
        print(f"Se obtuvieron {len(resultados)} filas.")

    # Cargar archivo base y procesar datos
    libro, hoja = cargar_archivo_excel(file_path)
    nuevas = insertar_sin_duplicados(hoja, resultados, columna_clave_idx=1)
    actualizar_tabla_excel(hoja)
    libro.save(file_path)
    print(f"{nuevas} nuevas filas añadidas. Archivo guardado correctamente en '{file_path}'.")

if __name__ == '__main__':
    main()
