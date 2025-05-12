    if "Portes" not in sheet.tables:
        print("No se encontró la tabla 'Portes'. Se aborta para no alterar el formato.")
        return

    tabla = sheet.tables["Portes"]
    ref = tabla.ref  # Ej. "A1:Z12"
    start_cell, end_cell = ref.split(":")
    start_row = int(''.join(filter(str.isdigit, start_cell)))
    end_row = int(''.join(filter(str.isdigit, end_cell)))

    insert_position = end_row + 1
    new_rows = []

    for row in resultados:
        if row[2] not in existing_invoice_codes:
            new_rows.append(row)

    if not new_rows:
        print("No hay nuevas filas para insertar.")
    else:
        print(f"Insertando {len(new_rows)} filas dentro de la tabla...")

        # Insertar tantas filas como necesarias justo debajo de la tabla
        sheet.insert_rows(insert_position, amount=len(new_rows))

        for idx, row_data in enumerate(new_rows):
            row_num = insert_position + idx
            for col_num, value in enumerate(row_data, 1):
                cell = sheet.cell(row=row_num, column=col_num, value=value)
                
                # Copiar formato de la fila anterior a la tabla
                template_row = sheet[row_num - 1]
                if col_num <= len(template_row):
                    cell.font = copy.copy(template_row[col_num - 1].font)
                    cell.fill = copy.copy(template_row[col_num - 1].fill)
                    cell.border = copy.copy(template_row[col_num - 1].border)
                    cell.alignment = copy.copy(template_row[col_num - 1].alignment)

        # Actualizar rango de la tabla "Portes"
        max_col = sheet.max_column
        last_col_letter = get_column_letter(max_col)
        new_ref = f"A1:{last_col_letter}{insert_position + len(new_rows) - 1}"
        tabla.ref = new_ref
        print(f"Tabla 'Portes' actualizada a rango: {new_ref}")
