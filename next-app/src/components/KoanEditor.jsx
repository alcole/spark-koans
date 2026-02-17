/**
 * Code Editor Component
 * Handles user code input with syntax highlighting and keyboard shortcuts
 */

import { useRef } from 'react';

export default function KoanEditor({ code, onChange, onRun }) {
  const textareaRef = useRef(null);

  const handleKeyDown = (e) => {
    // Tab key for indentation
    if (e.key === 'Tab') {
      e.preventDefault();
      const start = e.target.selectionStart;
      const end = e.target.selectionEnd;
      const newCode = code.substring(0, start) + '    ' + code.substring(end);
      onChange(newCode);
      setTimeout(() => {
        e.target.selectionStart = e.target.selectionEnd = start + 4;
      }, 0);
    }

    // Ctrl/Cmd + Enter to run
    if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') {
      e.preventDefault();
      onRun();
    }
  };

  return (
    <div className="bg-gray-900 rounded-lg border border-gray-800 overflow-hidden">
      <div className="flex items-center justify-between px-4 py-2 bg-gray-800 border-b border-gray-700">
        <span className="text-sm text-gray-400">Your Code</span>
        <span className="text-xs text-gray-600">Ctrl/Cmd+Enter to run</span>
      </div>
      <textarea
        ref={textareaRef}
        value={code}
        onChange={(e) => onChange(e.target.value)}
        onKeyDown={handleKeyDown}
        className="w-full h-64 p-4 bg-gray-950 text-gray-100 font-mono text-sm resize-none focus:outline-none focus:ring-2 focus:ring-orange-500/50"
        spellCheck={false}
      />
    </div>
  );
}
