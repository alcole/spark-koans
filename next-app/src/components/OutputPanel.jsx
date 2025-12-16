/**
 * Output Panel Component
 * Displays code execution results with appropriate styling
 */

export default function OutputPanel({ output }) {
  const getOutputStyle = () => {
    if (output.includes('🎉')) return 'text-green-400';
    if (output.includes('Error') || output.includes('❌')) return 'text-red-400';
    if (output.includes('✓')) return 'text-emerald-400';
    return 'text-gray-300';
  };

  return (
    <div className="bg-gray-900 rounded-lg border border-gray-800 overflow-hidden">
      <div className="px-4 py-2 bg-gray-800 border-b border-gray-700">
        <span className="text-sm text-gray-400">Output</span>
      </div>
      <pre
        className={`p-4 h-48 overflow-auto font-mono text-sm whitespace-pre-wrap ${getOutputStyle()}`}
      >
        {output || 'Output will appear here...'}
      </pre>
    </div>
  );
}
