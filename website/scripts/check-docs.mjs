import { readdir, readFile } from 'node:fs/promises'
import path from 'node:path'
import { fileURLToPath } from 'node:url'

const scriptDir = path.dirname(fileURLToPath(import.meta.url))
const repositoryRoot = path.resolve(scriptDir, '..', '..')
const docsRoot = path.join(repositoryRoot, 'website', 'docs')

const sourcePaths = {
  store: path.join(
    repositoryRoot,
    'testing/ratchet-tck/store/src/main/java/run/ratchet/tck/store/ConformanceLevel.java',
  ),
  api: path.join(
    repositoryRoot,
    'testing/ratchet-tck/api/src/main/java/run/ratchet/tck/api/ApiConformanceReportExtension.java',
  ),
  jakarta: path.join(
    repositoryRoot,
    'testing/ratchet-tck/jakarta/src/main/java/run/ratchet/tck/jakarta/JakartaConformanceReportExtension.java',
  ),
}

const docsPaths = {
  adoption: path.join(docsRoot, 'conformance', 'adopting-the-tck.md'),
  conformanceOverview: path.join(docsRoot, 'conformance', 'index.md'),
  storeGuide: path.join(docsRoot, 'advanced', 'spi-implementation.md'),
  sidebar: path.join(docsRoot, '.vitepress', 'config.ts'),
}

function contractNames(text) {
  return [...text.matchAll(/"(Abstract[A-Za-z0-9]+Contract)"/g)].map((match) => match[1])
}

function listOfBodies(text) {
  const bodies = []
  let searchFrom = 0

  while (true) {
    const marker = text.indexOf('List.of(', searchFrom)
    if (marker === -1) return bodies

    const bodyStart = marker + 'List.of('.length
    let depth = 1
    let quote = null
    let escaped = false

    for (let index = bodyStart; index < text.length; index += 1) {
      const character = text[index]
      if (quote !== null) {
        if (escaped) {
          escaped = false
        } else if (character === '\\') {
          escaped = true
        } else if (character === quote) {
          quote = null
        }
        continue
      }

      if (character === '"' || character === "'") {
        quote = character
      } else if (character === '(') {
        depth += 1
      } else if (character === ')') {
        depth -= 1
        if (depth === 0) {
          bodies.push(text.slice(bodyStart, index))
          searchFrom = index + 1
          break
        }
      }
    }

    if (depth !== 0) {
      throw new Error('Unbalanced List.of(...) while reading ConformanceLevel.java')
    }
  }
}

function storeInventory(source) {
  const levels = ['CORE', 'BEHAVIORAL', 'ADVANCED']
  const required = []
  const conditional = []

  for (let index = 0; index < levels.length; index += 1) {
    const start = source.indexOf(`${levels[index]}(`)
    const end =
      index + 1 < levels.length
        ? source.indexOf(`${levels[index + 1]}(`, start)
        : source.indexOf('private static final', start)
    if (start === -1 || end === -1) {
      throw new Error(`Could not locate ${levels[index]} in ConformanceLevel.java`)
    }

    const lists = listOfBodies(source.slice(start, end))
    if (lists.length !== 2) {
      throw new Error(`${levels[index]} must have one required and one conditional contract list`)
    }
    required.push(...contractNames(lists[0]))
    conditional.push(...contractNames(lists[1]))
  }

  return { required, conditional }
}

function uniqueCount(values, label, errors) {
  const unique = new Set(values)
  if (unique.size !== values.length) errors.push(`${label} contains duplicate contract names`)
  return unique.size
}

async function markdownFiles(directory) {
  const entries = await readdir(directory, { withFileTypes: true })
  const nested = await Promise.all(
    entries.map(async (entry) => {
      const fullPath = path.join(directory, entry.name)
      if (entry.isDirectory()) return markdownFiles(fullPath)
      return entry.isFile() && entry.name.endsWith('.md') ? [fullPath] : []
    }),
  )
  return nested.flat()
}

function githubHeadingSlugs(markdown) {
  const slugs = new Set()
  for (const match of markdown.matchAll(/^#{1,6}\s+(.+)$/gm)) {
    const slug = match[1]
      .replace(/<[^>]+>/g, '')
      .replace(/[`*_~]/g, '')
      .toLowerCase()
      .replace(/[^\p{L}\p{N}\-_ ]/gu, '')
      .trim()
      .replace(/\s+/g, '-')
    if (slug) slugs.add(slug)
  }
  return slugs
}

const [
  storeSource,
  apiSource,
  jakartaSource,
  adoption,
  conformanceOverview,
  storeGuide,
  sidebar,
  readme,
] = await Promise.all([
    readFile(sourcePaths.store, 'utf8'),
    readFile(sourcePaths.api, 'utf8'),
    readFile(sourcePaths.jakarta, 'utf8'),
    readFile(docsPaths.adoption, 'utf8'),
    readFile(docsPaths.conformanceOverview, 'utf8'),
    readFile(docsPaths.storeGuide, 'utf8'),
    readFile(docsPaths.sidebar, 'utf8'),
    readFile(path.join(repositoryRoot, 'README.md'), 'utf8'),
  ])

const errors = []
const store = storeInventory(storeSource)
const inventory = {
  storeRequired: uniqueCount(store.required, 'Store required registry', errors),
  storeConditional: uniqueCount(store.conditional, 'Store conditional registry', errors),
  api: uniqueCount(contractNames(apiSource), 'API registry', errors),
  jakarta: uniqueCount(contractNames(jakartaSource), 'Jakarta registry', errors),
}

for (const [name, count] of Object.entries(inventory)) {
  if (count === 0) errors.push(`Source conformance registry is unexpectedly empty: ${name}`)
}

const overviewInventoryClaim =
  'passes every required contract and every applicable conditional contract in the source registry'
if (!conformanceOverview.includes(overviewInventoryClaim)) {
  errors.push(`Conformance overview is missing registry-based inventory text`)
}

const adoptionTokens = [
  'JobStoreContractFixture',
  'ConformanceLevel.getRequiredContracts()',
  'ConformanceLevel.getOptionalContracts()',
  'RatchetTckRuntime',
  'supportsCallerTransactionRollback()',
  'generated API conformance report is the authoritative registry',
  'RatchetTckProbe',
  'ratchet.tck.store.name',
  'ratchet.tck.runtime.name',
  'target/tck-conformance-report.md',
  'target/tck-api-conformance-report.md',
  'target/tck-jakarta-conformance-report.md',
  'not a fourth self-certification tier',
  'cannot distinguish an omitted applicable conditional contract',
]
for (const token of adoptionTokens) {
  if (!adoption.includes(token)) errors.push(`Adoption guide is missing required seam: ${token}`)
}

const storeContractTokens = [
  'LockStore is a best-effort lease',
  'not a strict-exclusive lock',
  'fencing tokens',
  'AbstractLockStoreContract',
  'Transaction boundaries are part of the store contract',
  '@Transactional(REQUIRES_NEW)',
  'AbstractJobStoreTransactionBoundaryContract',
]
for (const token of storeContractTokens) {
  if (!storeGuide.includes(token)) errors.push(`Store guide is missing normative content: ${token}`)
}

if (!sidebar.includes("link: '/conformance/adopting-the-tck'")) {
  errors.push('Conformance adoption guide is not present in the VitePress sidebar')
}

const readmeSlugs = githubHeadingSlugs(readme)
const rootReadmeLink =
  /https:\/\/github\.com\/ratchet-run\/ratchet(?:\/blob\/[^/]+\/README\.md|\/)?#([A-Za-z0-9_-]+)/g
for (const markdownPath of await markdownFiles(docsRoot)) {
  const markdown = await readFile(markdownPath, 'utf8')
  for (const match of markdown.matchAll(rootReadmeLink)) {
    if (!readmeSlugs.has(match[1].toLowerCase())) {
      errors.push(
        `${path.relative(repositoryRoot, markdownPath)} links to missing README anchor #${match[1]}`,
      )
    }
  }
}

if (errors.length > 0) {
  throw new Error(`Documentation contract check failed:\n- ${errors.join('\n- ')}`)
}

console.log(
  `Documentation contracts OK: store ${inventory.storeRequired}+${inventory.storeConditional}, ` +
    `API ${inventory.api}, Jakarta ${inventory.jakarta}`,
)
