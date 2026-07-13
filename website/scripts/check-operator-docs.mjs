import { readFile, writeFile } from 'node:fs/promises'
import path from 'node:path'
import { fileURLToPath } from 'node:url'

const scriptDir = path.dirname(fileURLToPath(import.meta.url))
const repositoryRoot = path.resolve(scriptDir, '..', '..')
const docsRoot = path.join(repositoryRoot, 'website', 'docs')
const catalogPath = path.join(
  repositoryRoot,
  'ratchet-api/src/main/java/run/ratchet/api/internal/RatchetConfigKeys.java',
)
const referencePath = path.join(docsRoot, 'deployment', 'configuration-reference.md')
const startMarker = '<!-- CONFIG_REFERENCE_START -->'
const endMarker = '<!-- CONFIG_REFERENCE_END -->'

function findInitializerEnd(source, start) {
  let parentheses = 0
  let braces = 0
  let brackets = 0
  let quote = null
  let escaped = false

  for (let index = start; index < source.length; index += 1) {
    const character = source[index]
    if (quote !== null) {
      if (escaped) escaped = false
      else if (character === '\\') escaped = true
      else if (character === quote) quote = null
      continue
    }

    if (character === '"' || character === "'") quote = character
    else if (character === '(') parentheses += 1
    else if (character === ')') parentheses -= 1
    else if (character === '{') braces += 1
    else if (character === '}') braces -= 1
    else if (character === '[') brackets += 1
    else if (character === ']') brackets -= 1
    else if (character === ';' && parentheses === 0 && braces === 0 && brackets === 0) return index
  }

  throw new Error('Unterminated RatchetConfigKey initializer')
}

function firstCallArguments(initializer) {
  const open = initializer.indexOf('(')
  if (open === -1) throw new Error(`Configuration initializer has no call: ${initializer}`)

  const args = []
  let start = open + 1
  let parentheses = 1
  let braces = 0
  let brackets = 0
  let quote = null
  let escaped = false

  for (let index = start; index < initializer.length; index += 1) {
    const character = initializer[index]
    if (quote !== null) {
      if (escaped) escaped = false
      else if (character === '\\') escaped = true
      else if (character === quote) quote = null
      continue
    }

    if (character === '"' || character === "'") quote = character
    else if (character === '(') parentheses += 1
    else if (character === ')') {
      parentheses -= 1
      if (parentheses === 0) {
        args.push(initializer.slice(start, index).trim())
        return args
      }
    } else if (character === '{') braces += 1
    else if (character === '}') braces -= 1
    else if (character === '[') brackets += 1
    else if (character === ']') brackets -= 1
    else if (character === ',' && parentheses === 1 && braces === 0 && brackets === 0) {
      args.push(initializer.slice(start, index).trim())
      start = index + 1
    }
  }

  throw new Error(`Unbalanced configuration initializer: ${initializer}`)
}

function javaString(expression) {
  return JSON.parse(expression)
}

function displayDefault(expression) {
  const value = expression.trim()
  if (value.startsWith('"')) {
    const decoded = javaString(value)
    return decoded === '' ? '(empty string)' : decoded
  }
  if (/^-?\d[\d_]*(?:\.\d+)?[LF]?$/i.test(value)) {
    return value.replaceAll('_', '').replace(/[LF]$/i, '')
  }
  if (value === 'true' || value === 'false') return value
  return value.slice(value.lastIndexOf('.') + 1)
}

function fixedKeys(source) {
  const declaration = /public static final RatchetConfigKey<[^>]+>\s+([A-Z0-9_]+)\s*=/g
  const keys = []

  for (const match of source.matchAll(declaration)) {
    const initializerStart = source.indexOf('=', match.index) + 1
    const initializerEnd = findInitializerEnd(source, initializerStart)
    const initializer = source.slice(initializerStart, initializerEnd).trim()
    const args = firstCallArguments(initializer)
    if (args.length < 3) throw new Error(`${match[1]} has fewer than three catalog arguments`)

    const property = javaString(args[0])
    const environment = javaString(args[1])
    if (!property.startsWith('ratchet.')) throw new Error(`${match[1]} has invalid property ${property}`)
    if (!environment.startsWith('RATCHET_')) {
      throw new Error(`${match[1]} has invalid environment variable ${environment}`)
    }

    keys.push({
      constant: match[1],
      property,
      environment,
      defaultValue: displayDefault(args[2]),
    })
  }

  for (const field of ['property', 'environment', 'constant']) {
    const values = keys.map((key) => key[field])
    if (new Set(values).size !== values.length) throw new Error(`Duplicate configuration ${field}`)
  }
  if (keys.length === 0) throw new Error('No fixed RatchetConfigKey declarations found')
  return keys
}

function table(keys) {
  const rows = [
    '| Property | Environment variable | Default | Catalog key |',
    '|---|---|---|---|',
    ...keys.map(
      (key) =>
        `| \`${key.property}\` | \`${key.environment}\` | \`${key.defaultValue}\` | \`${key.constant}\` |`,
    ),
  ]
  return rows.join('\n')
}

function requireText(label, content, expected, errors) {
  if (!content.includes(expected)) errors.push(`${label} is missing: ${expected}`)
}

function forbidText(label, content, forbidden, errors) {
  if (content.includes(forbidden)) errors.push(`${label} must not contain: ${forbidden}`)
}

const source = await readFile(catalogPath, 'utf8')
const keys = fixedKeys(source)
const generated = table(keys)
if (process.argv.includes('--print')) {
  console.log(`This build exposes **${keys.length} fixed keys**`)
  console.log(generated)
  process.exit(0)
}

let reference = await readFile(referencePath, 'utf8')
const start = reference.indexOf(startMarker)
const end = reference.indexOf(endMarker)
if (start === -1 || end === -1 || end < start) {
  throw new Error('Configuration reference is missing its generated-table markers')
}

const actual = reference.slice(start + startMarker.length, end).trim()
if (process.argv.includes('--write')) {
  reference =
    reference.slice(0, start + startMarker.length) +
    `\n\n${generated}\n\n` +
    reference.slice(end)
  reference = reference.replace(
    /This build exposes \*\*\d+ fixed keys\*\*/,
    `This build exposes **${keys.length} fixed keys**`,
  )
  await writeFile(referencePath, reference)
  console.log(`Updated configuration reference with ${keys.length} fixed keys`)
  process.exit(0)
}

const errors = []
if (actual !== generated) {
  errors.push('configuration-reference.md is out of sync; run npm run docs:sync-config-reference')
}
requireText(
  'configuration-reference.md',
  reference,
  `This build exposes **${keys.length} fixed keys**`,
  errors,
)

const [
  sidebar,
  runtimeSetup,
  gettingStartedInstall,
  gettingStartedConfig,
  spiReference,
  adr,
] = await Promise.all([
  readFile(path.join(docsRoot, '.vitepress', 'config.ts'), 'utf8'),
  readFile(path.join(docsRoot, 'deployment', 'installation.md'), 'utf8'),
  readFile(path.join(docsRoot, 'getting-started', 'installation.md'), 'utf8'),
  readFile(path.join(docsRoot, 'getting-started', 'configuration.md'), 'utf8'),
  readFile(path.join(docsRoot, 'api-reference', 'spi-interfaces.md'), 'utf8'),
  readFile(path.join(docsRoot, 'adr', '0001-payload-encryption-threat-model.md'), 'utf8'),
])

requireText(
  'sidebar',
  sidebar,
  "{ text: 'Runtime setup', link: '/deployment/installation' }",
  errors,
)
requireText(
  'sidebar',
  sidebar,
  "{ text: 'Configuration reference', link: '/deployment/configuration-reference' }",
  errors,
)
forbidText('runtime setup', runtimeSetup, '<artifactId>ratchet-bom</artifactId>', errors)
for (const token of [
  '](/getting-started/installation)',
  'RatchetOptions',
  'ClassPolicy',
  'allowEmptyClassPolicy(true)',
]) {
  requireText('runtime setup', runtimeSetup, token, errors)
}
for (const token of ['](/deployment/installation)', 'RatchetOptions', 'ClassPolicy']) {
  requireText('getting-started installation', gettingStartedInstall, token, errors)
}
for (const token of [
  'RATCHET_REDACT_EMAILS',
  'RATCHET_MASK_PAYLOADS',
  '/deployment/configuration-reference',
]) {
  requireText('getting-started configuration', gettingStartedConfig, token, errors)
}
for (const token of [
  '`JobDetail` read APIs',
  'does not change the stored payload',
  'RATCHET_MASK_PAYLOADS',
]) {
  requireText('PayloadMaskingPolicy reference', spiReference, token, errors)
}
requireText('payload-encryption ADR', adr, '- **Status:** Accepted', errors)

if (errors.length > 0) {
  throw new Error(`Operator documentation contract check failed:\n- ${errors.join('\n- ')}`)
}

console.log(`Operator documentation contracts OK: ${keys.length} fixed configuration keys`)
