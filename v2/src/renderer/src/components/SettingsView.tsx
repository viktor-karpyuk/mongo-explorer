import { usePrefsStore } from '../store/prefs';

export function SettingsView() {
  const prefs = usePrefsStore((s) => s.prefs);
  const setTheme = usePrefsStore((s) => s.setTheme);
  const setEditorFontSize = usePrefsStore((s) => s.setEditorFontSize);
  const setTabWidth = usePrefsStore((s) => s.setTabWidth);

  return (
    <div className="view">
      <header className="view__header">
        <h1>Settings</h1>
      </header>

      <section className="panel">
        <h2>Appearance</h2>
        <div className="settings-row">
          <span>Theme</span>
          <div className="seg-control">
            <button
              className={`seg${prefs.theme === 'dark' ? ' seg--active' : ''}`}
              onClick={() => setTheme('dark')}
            >
              Dark
            </button>
            <button
              className={`seg${prefs.theme === 'light' ? ' seg--active' : ''}`}
              onClick={() => setTheme('light')}
            >
              Light
            </button>
          </div>
        </div>
      </section>

      <section className="panel">
        <h2>Editor</h2>
        <div className="settings-row">
          <span>Font size</span>
          <div className="settings-row__controls">
            <button
              className="btn btn--ghost btn--xs"
              onClick={() => setEditorFontSize(prefs.editorFontSize - 0.5)}
            >
              A−
            </button>
            <span style={{ fontVariantNumeric: 'tabular-nums', minWidth: 48, textAlign: 'center' }}>
              {prefs.editorFontSize.toFixed(1)} px
            </span>
            <button
              className="btn btn--ghost btn--xs"
              onClick={() => setEditorFontSize(prefs.editorFontSize + 0.5)}
            >
              A+
            </button>
          </div>
        </div>
        <div className="settings-row">
          <span>Tab width</span>
          <select
            value={prefs.tabWidth}
            onChange={(e) => setTabWidth(Number(e.target.value))}
          >
            <option value={2}>2 spaces</option>
            <option value={4}>4 spaces</option>
            <option value={8}>8 spaces</option>
          </select>
        </div>
      </section>

      <section className="panel">
        <h2>Keyboard shortcuts</h2>
        <table className="kbd-table">
          <tbody>
            <tr>
              <td>Run query</td>
              <td><kbd>⌘</kbd>/<kbd>Ctrl</kbd>+<kbd>↵</kbd></td>
            </tr>
            <tr>
              <td>Close dialog</td>
              <td><kbd>Esc</kbd></td>
            </tr>
            <tr>
              <td>Shell: send</td>
              <td><kbd>↵</kbd></td>
            </tr>
            <tr>
              <td>Shell: newline</td>
              <td><kbd>⇧</kbd>+<kbd>↵</kbd></td>
            </tr>
            <tr>
              <td>Shell: previous in history</td>
              <td><kbd>⌘</kbd>/<kbd>Ctrl</kbd>+<kbd>↑</kbd></td>
            </tr>
            <tr>
              <td>Shell: next in history</td>
              <td><kbd>⌘</kbd>/<kbd>Ctrl</kbd>+<kbd>↓</kbd></td>
            </tr>
          </tbody>
        </table>
        <p className="muted">Customizable bindings ship in a follow-up alpha.</p>
      </section>
    </div>
  );
}
