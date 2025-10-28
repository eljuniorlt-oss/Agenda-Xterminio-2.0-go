
import sqlite3
from contextlib import closing
from datetime import datetime, date, time as dtime
import calendar
import pandas as pd
import streamlit as st

DB_PATH = "agenda.db"

SCHEMA = """
CREATE TABLE IF NOT EXISTS clients (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    phone TEXT,
    address TEXT,
    notes TEXT
);
CREATE TABLE IF NOT EXISTS services (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    service_date TEXT NOT NULL,
    service_time TEXT,
    client_id INTEGER,
    service_type TEXT,
    amount REAL DEFAULT 0,
    status TEXT CHECK(status IN ('Pagado','Pendiente')) DEFAULT 'Pendiente',
    notes TEXT,
    FOREIGN KEY(client_id) REFERENCES clients(id)
);
"""

def init_db():
    with closing(sqlite3.connect(DB_PATH)) as con:
        cur = con.cursor()
        for stmt in SCHEMA.strip().split(";"):
            s = stmt.strip()
            if s:
                cur.execute(s + ";")
        con.commit()

@st.cache_data(ttl=5.0)
def get_clients_df():
    with closing(sqlite3.connect(DB_PATH)) as con:
        return pd.read_sql_query("SELECT id, name, phone, address, notes FROM clients ORDER BY name", con)

@st.cache_data(ttl=5.0)
def get_services_df(start=None, end=None):
    query = "SELECT s.id, s.service_date, s.service_time, c.name as client, c.phone, c.address, s.service_type, s.amount, s.status, s.notes, s.client_id FROM services s LEFT JOIN clients c ON c.id=s.client_id"
    params = []
    if start and end:
        query += " WHERE date(s.service_date) >= date(?) AND date(s.service_date) < date(?)"
        params = [start, end]
    query += " ORDER BY s.service_date, s.service_time"
    with closing(sqlite3.connect(DB_PATH)) as con:
        df = pd.read_sql_query(query, con, params=params, parse_dates=["service_date"])
    # 🔧 Mostrar solo la fecha sin hora en la tabla
    if not df.empty and "service_date" in df.columns:
        try:
            df["service_date"] = pd.to_datetime(df["service_date"]).dt.date
        except Exception:
            pass
    return df

def add_client(name, phone, address, notes):
    with closing(sqlite3.connect(DB_PATH)) as con:
        cur = con.cursor()
        cur.execute("INSERT INTO clients (name, phone, address, notes) VALUES (?,?,?,?)",
                    (name.strip(), phone.strip(), address.strip(), notes.strip()))
        con.commit()

def update_client(client_id, name, phone, address, notes):
    with closing(sqlite3.connect(DB_PATH)) as con:
        cur = con.cursor()
        cur.execute("""UPDATE clients SET name=?, phone=?, address=?, notes=? WHERE id=?""",
                    (name.strip(), phone.strip(), address.strip(), notes.strip(), int(client_id)))
        con.commit()

def delete_client(client_id):
    with closing(sqlite3.connect(DB_PATH)) as con:
        cur = con.cursor()
        # Primero ponemos a NULL client_id en services
        cur.execute("UPDATE services SET client_id=NULL WHERE client_id=?", (int(client_id),))
        cur.execute("DELETE FROM clients WHERE id=?", (int(client_id),))
        con.commit()

def add_service(service_date, service_time, client_id, service_type, amount, status, notes):
    with closing(sqlite3.connect(DB_PATH)) as con:
        cur = con.cursor()
        cur.execute("""INSERT INTO services
            (service_date, service_time, client_id, service_type, amount, status, notes)
            VALUES (?,?,?,?,?,?,?)""",
            (service_date, service_time, client_id, service_type, amount, status, notes))
        con.commit()

def get_service_by_id(service_id):
    with closing(sqlite3.connect(DB_PATH)) as con:
        cur = con.cursor()
        cur.execute("""SELECT id, service_date, service_time, client_id, service_type, amount, status, notes
                       FROM services WHERE id=?""", (int(service_id),))
        row = cur.fetchone()
        if not row:
            return None
        keys = ["id","service_date","service_time","client_id","service_type","amount","status","notes"]
        return dict(zip(keys, row))

def update_service(service_id, service_date, service_time, client_id, service_type, amount, status, notes):
    with closing(sqlite3.connect(DB_PATH)) as con:
        cur = con.cursor()
        cur.execute("""UPDATE services SET
                       service_date=?, service_time=?, client_id=?, service_type=?, amount=?, status=?, notes=?
                       WHERE id=?""",
                    (service_date, service_time, client_id, service_type, amount, status, notes, int(service_id)))
        con.commit()

def delete_service(service_id):
    with closing(sqlite3.connect(DB_PATH)) as con:
        cur = con.cursor()
        cur.execute("DELETE FROM services WHERE id=?", (int(service_id),))
        con.commit()

def update_cache():
    get_clients_df.clear()
    get_services_df.clear()

def month_bounds(year:int, month:int):
    first = date(year, month, 1)
    if month == 12:
        nxt = date(year+1, 1, 1)
    else:
        nxt = date(year, month+1, 1)
    return first, nxt

def export_excel(start, end):
    with closing(sqlite3.connect(DB_PATH)) as con:
        servicios = pd.read_sql_query("""
            SELECT s.service_date as Fecha, s.service_time as Hora, c.name as Cliente, c.phone as Telefono,
                   c.address as Direccion, s.service_type as Servicio, s.amount as Monto, s.status as Estatus, s.notes as Observaciones
            FROM services s LEFT JOIN clients c ON c.id=s.client_id
            WHERE date(s.service_date) >= date(?) AND date(s.service_date) < date(?)
            ORDER BY s.service_date, s.service_time
        """, con, params=[start, end])
        if not servicios.empty and "Fecha" in servicios.columns:
            try:
                servicios["Fecha"] = pd.to_datetime(servicios["Fecha"]).dt.date
            except Exception:
                pass
        clientes = pd.read_sql_query("""SELECT name as Cliente, phone as Telefono, address as Direccion, notes as Notas FROM clients ORDER BY name""", con)
    buf = pd.ExcelWriter("export_agenda.xlsx", engine="openpyxl")
    servicios.to_excel(buf, index=False, sheet_name="Agenda")
    clientes.to_excel(buf, index=False, sheet_name="Clientes")
    buf.close()
    return "export_agenda.xlsx"

def main():
    st.set_page_config(page_title="Agenda de Trabajo — Fumigaciones Xterminio", layout="wide")
    st.title("Agenda de Trabajo — Fumigaciones Xterminio")
    init_db()

    today = date.today()
    col1, col2 = st.sidebar.columns(2)
    year = col1.number_input("Año", min_value=2020, max_value=2100, value=today.year, step=1)
    month = col2.number_input("Mes", min_value=1, max_value=12, value=today.month, step=1)
    first, nxt = month_bounds(int(year), int(month))

    st.sidebar.markdown("### Filtros")
    status_filter = st.sidebar.multiselect("Estatus", ["Pendiente","Pagado"], default=["Pendiente","Pagado"])
    client_search = st.sidebar.text_input("Buscar cliente")

    tab_agregar, tab_agenda, tab_clientes, tab_resumen = st.tabs(["➕ Agregar", "📅 Agenda", "👥 Clientes", "📈 Resumen"])

    # --- TAB: Agregar ---
    with tab_agregar:
        st.subheader("Agregar servicio")
        clients_df = get_clients_df()
        client_names = ["(Nuevo cliente)"] + clients_df["name"].tolist()
        client_choice = st.selectbox("Cliente", client_names, index=0)

        new_name = new_phone = new_address = new_notes = ""
        client_id = None

        if client_choice == "(Nuevo cliente)":
            new_name = st.text_input("Nombre del cliente *")
            new_phone = st.text_input("Teléfono")
            new_address = st.text_input("Dirección")
            new_notes = st.text_area("Notas del cliente")
            if st.button("Guardar cliente nuevo"):
                if not new_name.strip():
                    st.error("El nombre del cliente es obligatorio.")
                else:
                    add_client(new_name, new_phone, new_address, new_notes)
                    update_cache()
                    st.success("Cliente guardado.")
        else:
            row = clients_df.loc[clients_df["name"] == client_choice].iloc[0]
            client_id = int(row["id"])
            st.info(f"Tel: {row['phone'] or ''} | Dir: {row['address'] or ''}")

        st.divider()
        service_date = st.date_input("Fecha del servicio", today, format="DD/MM/YYYY")
        service_time = st.time_input("Hora", value=dtime(10,0), step=300)
        service_type = st.text_input("Tipo de servicio", value="Fumigación general")
        amount = st.number_input("Monto", min_value=0.0, step=50.0, value=0.0)
        status = st.selectbox("Estatus de pago", ["Pendiente","Pagado"], index=0)
        notes = st.text_area("Observaciones")

        if st.button("Agregar servicio a la Agenda"):
            if client_choice == "(Nuevo cliente)":
                if not new_name.strip():
                    st.error("Debes especificar el nombre del cliente.")
                    st.stop()
                add_client(new_name, new_phone, new_address, new_notes)
                update_cache()
                clients_df = get_clients_df()
                client_row = clients_df.loc[clients_df["name"] == new_name].iloc[0]
                client_id = int(client_row["id"])
            add_service(service_date.isoformat(), service_time.strftime("%H:%M"),
                        client_id, service_type.strip(), float(amount), status, notes.strip())
            update_cache()
            st.success("Servicio agregado.")

    # --- TAB: Agenda ---
    with tab_agenda:
        st.subheader(f"Agenda — {calendar.month_name[int(month)]} {int(year)}")
        df = get_services_df(first.isoformat(), nxt.isoformat())
        if status_filter:
            df = df[df["status"].isin(status_filter)]
        if client_search.strip():
            df = df[df["client"].fillna("").str.contains(client_search.strip(), case=False, na=False)]

        if not df.empty:
            show_df = df.rename(columns={
                "service_date":"Fecha",
                "service_time":"Hora",
                "client":"Cliente",
                "phone":"Teléfono",
                "address":"Dirección",
                "service_type":"Servicio",
                "amount":"Monto",
                "status":"Estatus",
                "notes":"Observaciones",
                "id":"ID"
            })[["ID","Fecha","Hora","Cliente","Teléfono","Dirección","Servicio","Monto","Estatus","Observaciones"]]
            st.dataframe(show_df, use_container_width=True, hide_index=True)
        else:
            st.info("No hay servicios en el rango seleccionado.")

        st.markdown("---")
        st.subheader("Editar / Eliminar servicio")
        if not df.empty:
            service_ids = df["id"].tolist()
            selected_id = st.selectbox("Selecciona el ID del servicio", service_ids)
            s = get_service_by_id(selected_id)
            if s:
                try:
                    current_date = date.fromisoformat(str(s["service_date"])[:10])
                except Exception:
                    current_date = today
                try:
                    hh, mm = (str(s["service_time"] or "10:00")).split(":")[:2]
                    current_time = dtime(int(hh), int(mm))
                except Exception:
                    current_time = dtime(10,0)

                clients_df2 = get_clients_df()
                client_map = {row["name"]: int(row["id"]) for _, row in clients_df2.iterrows()}
                inv_client_map = {v:k for k,v in client_map.items()}
                current_client_name = inv_client_map.get(s["client_id"], "(Sin cliente)")
                client_name_edit = st.selectbox("Cliente", ["(Sin cliente)"] + list(client_map.keys()),
                                                index=(["(Sin cliente)"] + list(client_map.keys())).index(current_client_name) if current_client_name in (["(Sin cliente)"] + list(client_map.keys())) else 0)
                service_date_edit = st.date_input("Fecha", value=current_date, format="DD/MM/YYYY", key=f"edit_date_{selected_id}")
                service_time_edit = st.time_input("Hora", value=current_time, step=300, key=f"edit_time_{selected_id}")
                service_type_edit = st.text_input("Servicio", value=s["service_type"] or "", key=f"edit_type_{selected_id}")
                amount_edit = st.number_input("Monto", min_value=0.0, step=50.0, value=float(s["amount"] or 0.0), key=f"edit_amount_{selected_id}")
                status_edit = st.selectbox("Estatus", ["Pendiente","Pagado"], index=(0 if (s["status"]!="Pagado") else 1), key=f"edit_status_{selected_id}")
                notes_edit = st.text_area("Observaciones", value=s["notes"] or "", key=f"edit_notes_{selected_id}")

                colu1, colu2 = st.columns(2)
                if colu1.button("💾 Guardar cambios", key=f"save_{selected_id}"):
                    new_client_id = client_map.get(client_name_edit, None) if client_name_edit != "(Sin cliente)" else None
                    update_service(selected_id, service_date_edit.isoformat(),
                                   service_time_edit.strftime("%H:%M"),
                                   new_client_id, service_type_edit.strip(),
                                   float(amount_edit), status_edit, notes_edit.strip())
                    update_cache()
                    st.success("Servicio actualizado.")
                if colu2.button("🗑️ Eliminar servicio", key=f"del_{selected_id}"):
                    delete_service(selected_id)
                    update_cache()
                    st.warning("Servicio eliminado.")

        st.markdown("---")
        if st.button("Exportar a Excel (mes actual)"):
            file_path = export_excel(first.isoformat(), nxt.isoformat())
            with open(file_path, "rb") as f:
                st.download_button("Descargar export_agenda.xlsx", f, file_name="export_agenda.xlsx")

    # --- TAB: Clientes ---
    with tab_clientes:
        st.subheader("Directorio de clientes")
        cdf = get_clients_df()
        st.dataframe(cdf.rename(columns={"id":"ID","name":"Cliente","phone":"Teléfono","address":"Dirección","notes":"Notas"}),
                     use_container_width=True, hide_index=True)

        st.markdown("---")
        st.subheader("Editar / Eliminar cliente")
        if not cdf.empty:
            options = {f'{row["name"]} (ID {row["id"]})': int(row["id"]) for _, row in cdf.iterrows()}
            label = st.selectbox("Selecciona un cliente", list(options.keys()))
            cid = options[label]

            row = cdf[cdf["id"]==cid].iloc[0]
            name_edit = st.text_input("Cliente", value=row["name"] or "")
            phone_edit = st.text_input("Teléfono", value=row["phone"] or "")
            address_edit = st.text_input("Dirección", value=row["address"] or "")
            notes_edit = st.text_area("Notas", value=row["notes"] or "")

            col1, col2 = st.columns(2)
            if col1.button("💾 Guardar cambios (cliente)"):
                update_client(cid, name_edit, phone_edit, address_edit, notes_edit)
                update_cache()
                st.success("Cliente actualizado.")
            if col2.button("🗑️ Eliminar cliente"):
                delete_client(cid)
                update_cache()
                st.warning("Cliente eliminado (los servicios quedan sin cliente asignado).")

    # --- TAB: Resumen ---
    with tab_resumen:
        st.subheader("Resumen mensual")
        df = get_services_df(first.isoformat(), nxt.isoformat())
        ingresos_pagados = float(df.loc[df["status"]=="Pagado", "amount"].sum()) if not df.empty else 0.0
        cobros_pendientes = float(df.loc[df["status"]=="Pendiente", "amount"].sum()) if not df.empty else 0.0
        total_servicios = int(len(df)) if not df.empty else 0

        c1, c2, c3 = st.columns(3)
        c1.metric("Ingresos PAGADOS", f"${ingresos_pagados:,.2f}")
        c2.metric("Cobros PENDIENTES", f"${cobros_pendientes:,.2f}")
        c3.metric("Servicios en el mes", f"{total_servicios}")

        if not df.empty:
            counts = df.copy()
            if "service_date" in counts.columns:
                try:
                    counts["service_date"] = pd.to_datetime(counts["service_date"]).dt.date
                except Exception:
                    pass
            counts = counts.groupby("service_date").size().rename("Servicios").reset_index().rename(columns={"service_date":"Fecha"})
            st.bar_chart(counts.set_index("Fecha"))

if __name__ == "__main__":
    main()
