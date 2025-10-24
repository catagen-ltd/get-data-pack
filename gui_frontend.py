"""Standalone Tkinter GUI for configuring data pack processing."""

from __future__ import annotations

import tkinter as tk
from tkinter import filedialog, messagebox
from tkinter import ttk
from typing import Dict, List

from presets import DEFAULT_PRESETS, DEFAULT_PRESET_NAME


class DataPackGUI:
    """Presentation layer used to collect inputs before invoking the back end."""

    def __init__(self, root: tk.Tk) -> None:
        self.root = root
        self.root.title("Data Pack Processing")
        self.root.minsize(900, 600)

        # Local copy of preset definitions so we can extend them in-memory.
        self.presets: Dict[str, Dict[str, List[int | str]]] = {
            key: {
                "datalog_columns": value["datalog_columns"].copy(),
                "datalog_headers": value["datalog_headers"].copy(),
                "mfc_columns": value["mfc_columns"].copy(),
                "mfc_headers": value["mfc_headers"].copy(),
            }
            for key, value in DEFAULT_PRESETS.items()
        }

        self.datalog_path_var = tk.StringVar()
        self.mfc_path_var = tk.StringVar()
        self.preset_var = tk.StringVar(value=DEFAULT_PRESET_NAME)
        self.new_preset_name_var = tk.StringVar()
        self.status_var = tk.StringVar(value="Ready.")

        self._build_layout()
        self._populate_from_preset(DEFAULT_PRESET_NAME)

    def _build_layout(self) -> None:
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(1, weight=1)

        paths_frame = ttk.LabelFrame(self.root, text="Source Locations")
        paths_frame.grid(column=0, row=0, padx=16, pady=(16, 8), sticky="nsew")
        paths_frame.columnconfigure(1, weight=1)

        ttk.Label(paths_frame, text="Datalog file").grid(column=0, row=0, padx=8, pady=6, sticky="w")
        datalog_entry = ttk.Entry(paths_frame, textvariable=self.datalog_path_var, width=80)
        datalog_entry.grid(column=1, row=0, padx=8, pady=6, sticky="ew")
        ttk.Button(paths_frame, text="Browse", command=self._browse_datalog).grid(column=2, row=0, padx=8, pady=6)

        ttk.Label(paths_frame, text="MFC file").grid(column=0, row=1, padx=8, pady=6, sticky="w")
        mfc_entry = ttk.Entry(paths_frame, textvariable=self.mfc_path_var, width=80)
        mfc_entry.grid(column=1, row=1, padx=8, pady=6, sticky="ew")
        ttk.Button(paths_frame, text="Browse", command=self._browse_mfc).grid(column=2, row=1, padx=8, pady=6)

        preset_frame = ttk.LabelFrame(self.root, text="Preset Configuration")
        preset_frame.grid(column=0, row=1, padx=16, pady=8, sticky="nsew")
        preset_frame.columnconfigure(1, weight=1)
        preset_frame.columnconfigure(3, weight=1)
        preset_frame.rowconfigure(3, weight=1)

        ttk.Label(preset_frame, text="Preset").grid(column=0, row=0, padx=8, pady=6, sticky="w")
        self.preset_selector = ttk.Combobox(
            preset_frame,
            textvariable=self.preset_var,
            values=sorted(self.presets.keys()),
            state="readonly",
            width=30,
        )
        self.preset_selector.grid(column=1, row=0, padx=8, pady=6, sticky="w")
        self.preset_selector.bind("<<ComboboxSelected>>", self._on_preset_change)

        ttk.Label(preset_frame, text="Columns (comma or newline separated)").grid(
            column=0, row=1, padx=8, pady=6, sticky="w"
        )
        self.datalog_columns_text = tk.Text(preset_frame, height=8, width=40)
        self.datalog_columns_text.grid(column=0, row=2, padx=8, pady=6, sticky="nsew")

        ttk.Label(preset_frame, text="Headers (one per line)").grid(column=1, row=1, padx=8, pady=6, sticky="w")
        self.datalog_headers_text = tk.Text(preset_frame, height=8, width=40)
        self.datalog_headers_text.grid(column=1, row=2, padx=8, pady=6, sticky="nsew")

        ttk.Label(preset_frame, text="MFC Columns").grid(column=2, row=1, padx=8, pady=6, sticky="w")
        self.mfc_columns_text = tk.Text(preset_frame, height=8, width=30)
        self.mfc_columns_text.grid(column=2, row=2, padx=8, pady=6, sticky="nsew")

        ttk.Label(preset_frame, text="MFC Headers").grid(column=3, row=1, padx=8, pady=6, sticky="w")
        self.mfc_headers_text = tk.Text(preset_frame, height=8, width=30)
        self.mfc_headers_text.grid(column=3, row=2, padx=8, pady=6, sticky="nsew")

        controls_frame = ttk.Frame(preset_frame)
        controls_frame.grid(column=0, row=3, columnspan=4, padx=8, pady=(12, 4), sticky="ew")
        controls_frame.columnconfigure(1, weight=1)

        ttk.Label(controls_frame, text="New preset name").grid(column=0, row=0, padx=8, pady=4, sticky="w")
        new_name_entry = ttk.Entry(controls_frame, textvariable=self.new_preset_name_var, width=32)
        new_name_entry.grid(column=0, row=1, padx=8, pady=4, sticky="w")

        button_bar = ttk.Frame(controls_frame)
        button_bar.grid(column=1, row=0, rowspan=2, padx=8, pady=4, sticky="e")

        ttk.Button(button_bar, text="Save Preset", command=self._save_preset).grid(column=0, row=0, padx=6, pady=4)
        ttk.Button(button_bar, text="Reset", command=self._reset_fields).grid(column=1, row=0, padx=6, pady=4)
        ttk.Button(button_bar, text="Run", command=self._run_placeholder).grid(column=2, row=0, padx=6, pady=4)

        status_bar = ttk.Label(self.root, textvariable=self.status_var, anchor="w")
        status_bar.grid(column=0, row=2, padx=16, pady=(4, 16), sticky="ew")

    def _populate_from_preset(self, preset_name: str) -> None:
        preset = self.presets.get(preset_name)
        if not preset:
            return

        self._set_text_widget(self.datalog_columns_text, self._format_columns(preset["datalog_columns"]))
        self._set_text_widget(self.datalog_headers_text, "\n".join(preset["datalog_headers"]))
        self._set_text_widget(self.mfc_columns_text, self._format_columns(preset["mfc_columns"]))
        self._set_text_widget(self.mfc_headers_text, "\n".join(preset["mfc_headers"]))
        self.status_var.set(f"Loaded preset '{preset_name}'.")

    def _format_columns(self, columns: List[int | str]) -> str:
        return "\n".join(str(value) for value in columns)

    def _set_text_widget(self, widget: tk.Text, value: str) -> None:
        widget.configure(state="normal")
        widget.delete("1.0", tk.END)
        widget.insert(tk.END, value)
        widget.see("1.0")

    def _on_preset_change(self, _event: object) -> None:
        selected = self.preset_var.get()
        self._populate_from_preset(selected)

    def _browse_datalog(self) -> None:
        path = filedialog.askopenfilename(title="Select datalog file", filetypes=[("Data files", "*.tsv *.csv *.txt"), ("All files", "*.*")])
        if path:
            self.datalog_path_var.set(path)
            self.status_var.set("Datalog path selected.")

    def _browse_mfc(self) -> None:
        path = filedialog.askopenfilename(title="Select MFC file", filetypes=[("Data files", "*.tsv *.csv *.txt"), ("All files", "*.*")])
        if path:
            self.mfc_path_var.set(path)
            self.status_var.set("MFC path selected.")

    def _save_preset(self) -> None:
        preset_name = self.new_preset_name_var.get().strip() or self.preset_var.get()
        datalog_columns = self._parse_columns(self.datalog_columns_text.get("1.0", tk.END))
        datalog_headers = self._parse_headers(self.datalog_headers_text.get("1.0", tk.END))
        mfc_columns = self._parse_columns(self.mfc_columns_text.get("1.0", tk.END))
        mfc_headers = self._parse_headers(self.mfc_headers_text.get("1.0", tk.END))

        if not preset_name:
            messagebox.showerror("Missing Name", "Please enter a name for the preset.")
            return

        if not datalog_columns or not datalog_headers:
            messagebox.showerror("Incomplete Preset", "Datalog columns and headers are required.")
            return

        if len(datalog_columns) != len(datalog_headers):
            messagebox.showerror("Mismatch", "Datalog columns and headers must have the same length.")
            return

        if mfc_columns and len(mfc_columns) != len(mfc_headers):
            messagebox.showerror("Mismatch", "MFC columns and headers must have the same length or both be empty.")
            return

        self.presets[preset_name] = {
            "datalog_columns": datalog_columns,
            "datalog_headers": datalog_headers,
            "mfc_columns": mfc_columns,
            "mfc_headers": mfc_headers,
        }

        if preset_name not in self.preset_selector["values"]:
            updated_values = sorted(set(self.preset_selector["values"]) | {preset_name})
            self.preset_selector.configure(values=updated_values)

        self.preset_var.set(preset_name)
        self.new_preset_name_var.set("")
        self.status_var.set(f"Preset '{preset_name}' stored for this session.")

    def _run_placeholder(self) -> None:
        payload = {
            "datalog_path": self.datalog_path_var.get(),
            "mfc_path": self.mfc_path_var.get(),
            "preset": self.preset_var.get(),
            "datalog_columns": self._parse_columns(self.datalog_columns_text.get("1.0", tk.END)),
            "datalog_headers": self._parse_headers(self.datalog_headers_text.get("1.0", tk.END)),
            "mfc_columns": self._parse_columns(self.mfc_columns_text.get("1.0", tk.END)),
            "mfc_headers": self._parse_headers(self.mfc_headers_text.get("1.0", tk.END)),
        }

        summary = (
            "Inputs collected:\n"
            f"- Datalog: {payload['datalog_path'] or 'not set'}\n"
            f"- MFC: {payload['mfc_path'] or 'not set'}\n"
            f"- Preset: {payload['preset']}\n"
            f"- Datalog columns: {len(payload['datalog_columns'])}\n"
            f"- Datalog headers: {len(payload['datalog_headers'])}\n"
            f"- MFC columns: {len(payload['mfc_columns'])}\n"
            f"- MFC headers: {len(payload['mfc_headers'])}"
        )
        messagebox.showinfo("Preview", summary)
        self.status_var.set("Configuration captured. Backend integration pending.")

    def _reset_fields(self) -> None:
        self.datalog_path_var.set("")
        self.mfc_path_var.set("")
        self.new_preset_name_var.set("")
        self.preset_var.set(DEFAULT_PRESET_NAME)
        self._populate_from_preset(DEFAULT_PRESET_NAME)
        self.status_var.set("Reset to defaults.")

    def _parse_columns(self, raw_value: str) -> List[int]:
        columns: List[int] = []
        for piece in raw_value.replace("\n", ",").split(","):
            value = piece.strip()
            if not value:
                continue
            try:
                columns.append(int(value))
            except ValueError:
                messagebox.showerror("Invalid Column", f"Cannot convert '{value}' into an integer index.")
                return []
        return columns

    def _parse_headers(self, raw_value: str) -> List[str]:
        return [line.strip() for line in raw_value.splitlines() if line.strip()]


def launch() -> None:
    root = tk.Tk()
    DataPackGUI(root)
    root.mainloop()


if __name__ == "__main__":
    launch()
