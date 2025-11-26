
import os
from datetime import date, time as dtime
import calendar
import pandas as pd
import streamlit as st
from supabase import create_client, Client

SB_URL = st.secrets["SUPABASE_URL"]
SB_KEY = st.secrets["SUPABASE_KEY"]
supabase: Client = create_client(SB_URL, SB_KEY)

def get_clients_df():
    res = supabase.table("clients").select("id,name,phone,address,notes").order("name").execute()
    data = res.data or []
    return pd.DataFrame(data, columns=["id","name","phone","address","notes"])

def get_services_df(start=None, end=None):
    q = supabase.table("services").select("id,service_date,service_time,client_id,service_type,amount,status,notes, clients(name,phone,address)").order("service_date").order("service_time", desc=False)
    if start and end:
        q = q.gte("service_date", start).lt("service_date", end)
    res = q.execute()
    rows = res.data or []
    for r in rows:
        c = r.get("clients") or {}
        r["client"] = c.get("name")
        r["phone"] = c.get("phone")
        r["address"] = c.get("address")
    df = pd.DataFrame(rows, columns=["id","service_date","service_time","client","phone","address","service_type","amount","status","notes","client_id"])
    if not df.empty:
        df["service_date"] = pd.to_datetime(df["service_date"]).dt.date
    return df

def add_client(name, phone, address, notes):
    payload = {"name": name.strip(), "phone": phone.strip(), "address": address.strip(), "notes": notes.strip()}
    res = supabase.table("clients").insert(payload).execute()
    return res.data[0]["id"]

def update_client(client_id, name, phone, address, notes):
    supabase.table("clients").update({"name":name.strip(), "phone":phone.strip(), "address":address.strip(), "notes":notes.strip()}).eq("id", client_id).execute()

def delete_client(client_id):
    supabase.table("services").update({"client_id": None}).eq("client_id", client_id).execute()
    supabase.table("clients").delete().eq("id", client_id).execute()

def add_service(service_date, service_time, client_id, service_type, amount, status, notes):
    payload = {
        "service_date": service_date,
        "service_time": service_time,
        "client_id": client_id,
        "service_type": service_type.strip(),
        "amount": float(amount),
        "status": status,
        "notes": notes.strip()
    }
    supabase.table("services").insert(payload).execute()

def get_service_by_id(service_id):
    res = supabase.table("services").select("*").eq("id", service_id).single().execute()
    return res.data

def update_service(service_id, service_date, service_time, client_id, service_type, amount, status, notes):
    supabase.table("services").update({
        "service_date": service_date,
        "service_time": service_time,
        "client_id": client_id,
        "service_type": service_type.strip(),
        "amount": float(amount),
        "status": status,
        "notes": notes.strip()
    }).eq("id", service_id).execute()

def delete_service(service_id):
    supabase.table("services").delete().eq("id", service_id).execute()

def month_bounds(year:int, month:int):
    first = date(year, month, 1)
    if month == 12:
        nxt = date(year+1, 1, 1)
    else:
        nxt = date(year, month+1, 1)
    return first, nxt

def export_excel(start, end):
    df = get_services_df(start, end)
    clientes = get_clients_df()
    out1 = df.rename(columns={
        "service_date":"Fecha",
        "service_time":"Hora",
        "client":"Cliente",
        "phone":"Teléfono",
        "address":"Dirección",
        "service_type":"Servicio",
        "amount":"Monto",
        "status":"Estatus",
        "notes":"Observaciones",
    })[["Fecha","Hora","Cliente","Teléfono","Dirección","Servicio","Monto","Estatus","Observaciones"]]
    out2 = clientes.rename(columns={"name":"Cliente","phone":"Teléfono","address":"Dirección","notes":"Notas"})[["Cliente","Teléfono","Dirección","Notas"]]
    with pd.ExcelWriter("export_agenda.xlsx", engine="openpyxl") as xw:
        out1.to_excel(xw, index=False, sheet_name="Agenda")
        out2.to_excel(xw, index=False, sheet_name="Clientes")
    return "export_agenda.xlsx"

def main():
    st.set_page_config(page_title="Agenda — Fumigaciones Xterminio (Supabase)", layout="wide")
    st.title("Agenda de Trabajo — Fumigaciones Xterminio")
    today = date.today()

    col1, col2 = st.sidebar.columns(2)
    year = col1.number_input("Año", min_value=2020, max_value=2100, value=today.year, step=1)
    month = col2.number_input("Mes", min_value=1, max_value=12, value=today.month, step=1)
    first, nxt = month_bounds(int(year), int(month))

    st.sidebar.markdown("### Filtros")
    status_filter = st.sidebar.multiselect("Estatus", ["Pendiente","Pagado"], default=["Pendiente","Pagado"])
    client_search = st.sidebar.text_input("Buscar cliente")

    tab_agregar, tab_agenda, tab_clientes, tab_resumen = st.tabs(["➕ Agregar", "📅 Agenda", "👥 Clientes", "📈 Resumen"])

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
                    client_id = add_client(new_name, new_phone, new_address, new_notes)
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
            if client_choice == "(Nuevo cliente)" and client_id is None:
                if not new_name.strip():
                    st.error("Debes especificar el nombre del cliente.")
                    st.stop()
                client_id = add_client(new_name, new_phone, new_address, new_notes)
            add_service(service_date.isoformat(), service_time.strftime("%H:%M"),
                        client_id, service_type.strip(), float(amount), status, notes.strip())
            st.success("Servicio agregado.")

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
                from datetime import datetime as pydt
                try:
                    current_date = pydt.fromisoformat(str(s["service_date"])).date()
                except Exception:
                    current_date = date.today()
                try:
                    hh, mm = (s.get("service_time") or "10:00").split(":")[:2]
                    current_time = dtime(int(hh), int(mm))
                except Exception:
                    current_time = dtime(10,0)

                clients_df2 = get_clients_df()
                client_map = {row["name"]: int(row["id"]) for _, row in clients_df2.iterrows()}
                inv = {v:k for k,v in client_map.items()}
                current_client_name = inv.get(s.get("client_id"), "(Sin cliente)")
                client_name_edit = st.selectbox("Cliente", ["(Sin cliente)"] + list(client_map.keys()),
                                                index=(["(Sin cliente)"] + list(client_map.keys())).index(current_client_name) if current_client_name in (["(Sin cliente)"] + list(client_map.keys())) else 0)
                service_date_edit = st.date_input("Fecha", value=current_date, format="DD/MM/YYYY", key=f"edit_date_{selected_id}")
                service_time_edit = st.time_input("Hora", value=current_time, step=300, key=f"edit_time_{selected_id}")
                service_type_edit = st.text_input("Servicio", value=s.get("service_type") or "", key=f"edit_type_{selected_id}")
                amount_edit = st.number_input("Monto", min_value=0.0, step=50.0, value=float(s.get("amount") or 0.0), key=f"edit_amount_{selected_id}")
                status_edit = st.selectbox("Estatus", ["Pendiente","Pagado"], index=(0 if (s.get("status")!="Pagado") else 1), key=f"edit_status_{selected_id}")
                notes_edit = st.text_area("Observaciones", value=s.get("notes") or "", key=f"edit_notes_{selected_id}")

                colu1, colu2 = st.columns(2)
                if colu1.button("💾 Guardar cambios", key=f"save_{selected_id}"):
                    new_client_id = client_map.get(client_name_edit, None) if client_name_edit != "(Sin cliente)" else None
                    update_service(selected_id, service_date_edit.isoformat(),
                                   service_time_edit.strftime("%H:%M"),
                                   new_client_id, service_type_edit.strip(),
                                   float(amount_edit), status_edit, notes_edit.strip())
                    st.success("Servicio actualizado.")
                if colu2.button("🗑️ Eliminar servicio", key=f"del_{selected_id}"):
                    delete_service(selected_id)
                    st.warning("Servicio eliminado.")

        st.markdown("---")
        if st.button("Exportar a Excel (mes actual)"):
            file_path = export_excel(first.isoformat(), nxt.isoformat())
            with open(file_path, "rb") as f:
                st.download_button("Descargar export_agenda.xlsx", f, file_name="export_agenda.xlsx")

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
                st.success("Cliente actualizado.")
            if col2.button("🗑️ Eliminar cliente"):
                delete_client(cid)
                st.warning("Cliente eliminado (los servicios quedan sin cliente asignado).")

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
            counts = df.groupby("service_date").size().rename("Servicios").reset_index().rename(columns={"service_date":"Fecha"})
            st.bar_chart(counts.set_index("Fecha"))

if __name__ == "__main__":
    main()
