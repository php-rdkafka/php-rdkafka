#!/usr/bin/env php
<?php

/*
 * Rewrites the <contents> section of package.xml from the files tracked by git:
 *
 * - doc:  the documents listed in DOC_FILES
 * - src:  top-level *.c, *.h, *.stub.php, config.m4 and config.w32
 * - test: everything under tests/
 */

const DOC_FILES = ['CREDITS', 'LICENSE', 'README.md', 'UPGRADE.md'];
const SRC_FILES = ['config.m4', 'config.w32'];
const SRC_PATTERN = '/^[^\/]+\.(c|h|stub\.php)$/';

$root = dirname(__DIR__);
$packageFile = $root.'/package.xml';

$files = gitFiles($root);

$tree = [];
foreach ($files as $file) {
    $role = roleOf($file);
    if ($role === null) {
        continue;
    }
    $node = &$tree;
    $parts = explode('/', $file);
    $name = array_pop($parts);
    foreach ($parts as $dir) {
        $node = &$node['dirs'][$dir];
    }
    $node['files'][$name] = $role;
    unset($node);
}

$contents = " <contents>\n  <dir name=\"/\">\n".renderDir($tree, '   ')."  </dir>\n </contents>";

$xml = file_get_contents($packageFile);
if ($xml === false) {
    throw new \Exception('Failed loading package.xml');
}

$updated = preg_replace('#^ <contents>.*?^ </contents>#ms', $contents, $xml, -1, $count);
if ($count !== 1) {
    throw new \Exception(sprintf('Expected one <contents> section in package.xml, found %d', $count));
}

file_put_contents($packageFile, $updated);

/** @return list<string> */
function gitFiles(string $root): array
{
    $cmd = sprintf('git -C %s ls-files -z', escapeshellarg($root));
    $output = shell_exec($cmd);
    if (!is_string($output)) {
        throw new \Exception(sprintf('Command `%s` failed', $cmd));
    }

    return array_values(array_filter(explode("\0", $output), 'strlen'));
}

function roleOf(string $file): ?string
{
    if (in_array($file, DOC_FILES, true)) {
        return 'doc';
    }
    if (in_array($file, SRC_FILES, true) || preg_match(SRC_PATTERN, $file)) {
        return 'src';
    }
    if (str_starts_with($file, 'tests/')) {
        return 'test';
    }

    return null;
}

/** @param array{dirs?: array<string, array>, files?: array<string, string>} $dir */
function renderDir(array $dir, string $indent): string
{
    $files = $dir['files'] ?? [];
    $dirs = $dir['dirs'] ?? [];
    ksort($files, SORT_STRING);
    ksort($dirs, SORT_STRING);

    $output = '';
    foreach ($files as $name => $role) {
        $output .= sprintf("%s<file role=\"%s\" name=\"%s\"/>\n", $indent, $role, htmlspecialchars($name, ENT_XML1));
    }
    foreach ($dirs as $name => $child) {
        $output .= sprintf(
            "%s<dir name=\"%s\">\n%s%s</dir>\n",
            $indent,
            htmlspecialchars($name, ENT_XML1),
            renderDir($child, $indent.' '),
            $indent,
        );
    }

    return $output;
}
