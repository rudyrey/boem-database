/**
 * Reusable multi-select dropdown with checkboxes.
 *
 * Usage:
 *   const ms = new MultiSelect({
 *     container: document.getElementById('my-el'),
 *     label: 'Status',
 *     options: [{ value: 'A', label: 'Active' }, ...],
 *     onChange: (selectedValues) => { ... },
 *   });
 *   ms.getValues();  // ['A', 'C']
 *   ms.clear();
 *   ms.destroy();
 */
export class MultiSelect {
  constructor({ container, label, options, onChange }) {
    this.container = container;
    this.label = label;
    this.options = options;
    this.onChange = onChange;
    this._open = false;
    this._onOutsideClick = (e) => {
      if (!this.el.contains(e.target)) this.close();
    };
    this._render();
  }

  _render() {
    this.el = document.createElement('div');
    this.el.className = 'multi-select';

    this.btn = document.createElement('button');
    this.btn.className = 'multi-select-btn';
    this.btn.type = 'button';
    this.btn.textContent = this.label;
    this.btn.addEventListener('click', () => this.toggle());

    this.panel = document.createElement('div');
    this.panel.className = 'multi-select-panel';
    this.panel.style.display = 'none';

    for (const opt of this.options) {
      const lbl = document.createElement('label');
      lbl.className = 'multi-select-option';
      const cb = document.createElement('input');
      cb.type = 'checkbox';
      cb.value = opt.value;
      cb.addEventListener('change', () => this._onCheck());
      lbl.appendChild(cb);
      lbl.appendChild(document.createTextNode(' ' + opt.label));
      this.panel.appendChild(lbl);
    }

    this.el.appendChild(this.btn);
    this.el.appendChild(this.panel);
    this.container.appendChild(this.el);
  }

  toggle() {
    this._open ? this.close() : this.open();
  }

  open() {
    this._open = true;
    this.panel.style.display = '';
    this.btn.classList.add('open');
    document.addEventListener('click', this._onOutsideClick, true);
  }

  close() {
    this._open = false;
    this.panel.style.display = 'none';
    this.btn.classList.remove('open');
    document.removeEventListener('click', this._onOutsideClick, true);
  }

  _onCheck() {
    const vals = this.getValues();
    if (vals.length > 0) {
      this.btn.textContent = `${this.label} (${vals.length})`;
      this.btn.classList.add('has-selection');
    } else {
      this.btn.textContent = this.label;
      this.btn.classList.remove('has-selection');
    }
    if (this.onChange) this.onChange(vals);
  }

  getValues() {
    return [...this.panel.querySelectorAll('input:checked')].map(el => el.value);
  }

  clear() {
    this.panel.querySelectorAll('input:checked').forEach(el => { el.checked = false; });
    this.btn.textContent = this.label;
    this.btn.classList.remove('has-selection');
  }

  destroy() {
    this.close();
    this.el.remove();
  }
}
