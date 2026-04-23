-- Fix emoji storage for msg.message
-- Run on your target database before restarting the Nest service.

SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci;

ALTER TABLE `msg`
  CONVERT TO CHARACTER SET utf8mb4
  COLLATE utf8mb4_unicode_ci;

ALTER TABLE `msg`
  MODIFY COLUMN `message` LONGTEXT
  CHARACTER SET utf8mb4
  COLLATE utf8mb4_unicode_ci
  NOT NULL;

